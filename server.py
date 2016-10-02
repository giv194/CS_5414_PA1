#!/usr/bin/env python

"""
An echo server that uses threads to handle multiple clients at a time.
Entering any line of input at the terminal will exit the server.
"""

import select
import socket
import sys
import threading
import subprocess
import time
import copy
import json
from socket import error as SocketError
import errno

process_lock = threading.Lock()
thread_list = []

BASE_STATE = {
    "vote": False,
    "crashAfterVote": False,
    "crashAfterAck": False,
    "crashVoteReq": False,
    "crashPartialCommit": False,
    "crashPartialPreCommit": False
}

# Timeout value
TIMEOUT = 2.5
HB_TIMEOUT = 0.2*TIMEOUT

GPORT = 20000

# PROCCESS MESSAGE TYPES:
I_AM_COORDINATOR = "I_AM_COORDINATOR"
ELECTION_ACK = "ELECTION_ACK"
UR_ELECTED = "UR_ELECTED"
HEARTBEAT = "HEARTBEAT"
HEARTBEAT_ACK = "HEARTBEAT_ACK"
VOTE_REQ = "VOTE_REQ"
SHOULD_ABORT = "SHOULD_ABORT"
VOTE_YES = "VOTE_YES"
PRE_COMMIT = "PRE_COMMIT"
PRE_COMMIT_ACK = "PRE_COMMIT_ACK"
COMMIT = "COMMIT"

# 3PC stages
VOTE_STAGE = 1
PRECOMMIT_STAGE = 2
COMMIT_STAGE = 3

def check_port(port):
    return port >= GPORT and port < (GPORT+10000)

def parse_process_message(request):
    split = request.split(" ")
    data = {"message":""}
    initial_space = True
    for s in split:
        values = s.split("=")
        if len(values) == 1:
            if initial_space:
                initial_space = False
                data["command"] = values[0]
            else:
                data["message"] += values[0]+" "
        elif values[0]=="id":
            data[values[0]] = int(values[1])
        else:
            data[values[0]] = values[1]
    return data

class TimeOut():
    def __init__(self, time_out_period = TIMEOUT):
        self.time_out_period = time_out_period
        self.end = time.time()+time_out_period
    def waiting(self):
        return time.time() < self.end
    def reset(self):
        self.end = time.time() + self.time_out_period

class Server(threading.Thread):
    def __init__(self, port, process):
        threading.Thread.__init__(self)
        self.process = process
        self.host = ''
        self.port = port
        self.backlog = 5
        self.size = 1024
        self.server = None
        self.threads = []

    def open_socket(self):
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.host,self.port))
            self.server.listen(5)
        except socket.error, (value,message):
            if self.server:
                self.server.close()
            print "Could not open server socket: " + message
            sys.exit(1)


    def run(self):
        self.open_socket()
        input = [self.server,sys.stdin]
        running = 1
        t_o = TimeOut()
        hb_t_0 = TimeOut(HB_TIMEOUT)
        c_t_o = TimeOut(HB_TIMEOUT)
        p_t_o = TimeOut()
        while running:
            if check_port(self.port):
                if self.process.pc_stage == 0:
                    c_t_o.reset()
                    p_t_o.reset()
                    if not self.process.is_coordinator():
                        if not t_o.waiting():
                            print "timeout"
                            self.process.elect_coordinator()
                            t_o.reset()
                    else:
                        if not hb_t_0.waiting():
                            for p_id in range(0,self.process.n):
                                if p_id != self.process.id:
                                    try:
                                        Connection_Client(GPORT+p_id, self.process.id, HEARTBEAT).run()
                                    except:
                                        donothing = 0
                            hb_t_0.reset()
                else:
                    self.process.do_3pc(p_t_o, c_t_o)
            inputready,outputready,exceptready = select.select(input,[],[], 0)
            for s in inputready:
                if s == self.server:
                    # handle the server socket
                    c = Client(self.server.accept(), self.process, self.port, t_o)
                    c.start()
                    self.threads.append(c)
                    if not check_port(self.port):
                        self.process.m_client = c

                elif s == sys.stdin:
                    # handle standard input
                    junk = sys.stdin.readline()
                    print "Shutting down server %d ..."%(self.port%20000)
                    running = 0
            time.sleep(0.2*HB_TIMEOUT)

        # close all threads
        self.server.close()
        for c in self.threads:
            c.join()

    def crash(self):
        for c in self.threads:
            c.join()

class Client(threading.Thread):
    def __init__(self,(client,address), process, port, t_o=None):
        threading.Thread.__init__(self)
        self.client = client
        self.address = address
        self.port = port
        self.size = 1024
        self.process = process
        self.server_t_o = t_o

        # print 'Got connection from', address, ':', port

    def run(self):
        running = 1
        while running:
            try:
                data = self.client.recv(self.size)
                if data:
                    #echo server:
                    #self.client.send(data)
                    # print data, ':', self.port
                    process_lock.acquire()
                    to_return = self.process.process_command(data, self.port)
                    if to_return != None:
                        self.client.send(str(to_return) + "\n")
                    process_lock.release()

                    if check_port(self.port):
                        split = data.split(" ")
                        if split[0] == HEARTBEAT:
                            p_id = int([s for s in split if "id=" in s][0].split("=")[1])
                            if p_id != self.process.coordinator:
                                print "SETTING COORDINATOR ", p_id
                                with open("output_"+str(self.process.id) +".txt", "a+") as myfile:
                                    myfile.write("Setting new coordinator "+str(p_id)+"\n" )
                            self.process.coordinator = p_id
                            self.server_t_o.reset()
                else:
                    self.client.close()
                    running = 0
            except SocketError as e:
                if e.errno != errno.ECONNRESET:
                    raise # Not error we are looking for
                running = 0

class Connection_Client(threading.Thread):
    def __init__(self, port, p_id, message):
        self.port = port
        self.p_id = p_id
        self.message = message

    def run(self):
        host = 'localhost'
        size = 1024
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, self.port))
        if check_port(self.port):
            self.message += " id="+str(self.p_id)
        s.send(self.message)
        s.close()

    def crash(self):
        sys.exit(0)


class Process():
    # We should probably make this a monitor.
    def __init__(self, p_id, n, self_port, master_port):
        self.dt_index = 0
        self.coordinator = None
        self.id = p_id
        self.n = n
        self.port = self_port
        self.m_port = master_port
        self.m_client = None
        self.songs = {}
        self.up_set = set([x for x in range(n)])

        self.pc_stage = 0
        self.prev_stage = 0
        self.states = {}

        #Test cases:
        self.master_commands= copy.deepcopy(BASE_STATE)

        with open("output_"+ str(self.id)+".txt", "w") as myfile:
            myfile.write("")

    def is_coordinator(self):
        return self.coordinator == self.id

    def process_command(self, command_string, port):
        command_string = command_string.replace("\n", "")
        c_array = command_string.split(" ")
        if(port == self.m_port): #PROCESS MASTER COMMANDS:
            print 'Master command is: ' + (" ".join(c_array))
            #Crash Command:
            command = c_array[0]
            if command == "crash":
                print (" ".join(c_array))
                self.crash()

            # FLag Commands:
            elif command == "vote" and c_array[1] == "NO":
                print(" ".join(c_array))
                self.master_commands[command] = True
            elif command == "crashAfterVote":
                print (" ".join(c_array))
                self.master_commands[command] = True
            elif command == "crashAfterAck":
                print(" ".join(c_array))
                self.master_commands[command] = True
            elif command == "crashVoteReq":
                print(" ".join(c_array))
                self.master_commands[command] = [int(i) for i in c_array[1:]]
            elif command == "crashPartialPreCommit":
                print(" ".join(c_array))
                self.master_commands[command] = [int(i) for i in c_array[1:]]
            elif command == "crashPartialCommit":
                print(" ".join(c_array))
                self.master_commands[command] = [int(i) for i in c_array[1:]]

            # Get Command
            elif command == "get":
                try:
                    to_return = "resp " + str(self.songs[c_array[1]])
                except:
                    to_return = "resp NONE"
                return to_return

            # 3PC commands:
            elif command == "add"  and self.coordinator == self.id:
                # self.songs[c_array[1]] = c_array[2]
                self.master_commands["commit"] = command_string
                print c_array
                self.send_req(VOTE_REQ, VOTE_STAGE, command_string)
                return None
            elif command == "delete" and self.coordinator == self.id:
                # self.songs = {key: value for key, value in self.songs.items() if key != c_array[1]}
                self.master_commands["commit"] = command_string
                print c_array[1:]
                self.send_req(VOTE_REQ, VOTE_STAGE, command_string)
                return None
        else:
            # commands from process to coordinator
            if VOTE_YES in c_array[0]:
                data = parse_process_message(command_string)
                self.states[data["id"]] = True
                print self.states

            if PRE_COMMIT_ACK in c_array[0]:
                data = parse_process_message(command_string)
                self.states[data["id"]] = True

            # commands from coordinator to process
            if VOTE_REQ in c_array[0]:
                 self.recieve_request(command_string)

            if PRE_COMMIT in c_array[0]:
                self.recieve_request(command_string)

            if COMMIT in c_array[0]:
                self.recieve_request(command_string)

            if HEARTBEAT not in c_array[0]:
                print 'Process ', port, ' wants your attention'
                print c_array[0]
        # return 'wtf?'
        return "ack abort"

    # def create_request(self, c_array):
    #     request = "commit=" + c_array[0] + " name=" + c_array[1]
    #     if len(c_array) == 3:
    #         request +=  " url=" + c_array[2]
    #     print request
    #     return request

    def crash(self):
        subprocess.Popen(['./kill_script', str(self.m_port)], stdout=open('/dev/null'), stderr=open('/dev/null'))


    def elect_coordinator(self):
        if self.coordinator == None:
            self.coordinator = 0
        else:
            self.coordinator = (self.coordinator + 1) % self.n
        print "ELECTING NEW COORDINATOR: "+str(self.coordinator)
        if self.coordinator == self.id:
            print "I AM THE COORDINATOR"
            with open("output_"+str(self.id)+".txt", "a+") as myfile:
                myfile.write("I AM THE COORDINATOR "+str(self.id)+"\n" )
            if self.m_client:
                self.m_client.client.send("coordinator " + str(self.id) + "\n")

    # 3PC methods
    def do_3pc(self, p_t_o, c_t_o):
        # check if coordinator
        if self.is_coordinator():
            if not c_t_o.waiting():
                # coordinator VOTE REQ STAGE
                if self.pc_stage == VOTE_STAGE:
                    should_abort = False
                    if "vote" in self.master_commands:
                        should_abort = self.master_commands["vote"]
                    for p_id in self.up_set:
                        if p_id != self.coordinator and ( p_id not in self.states or self.states[p_id] == False ):
                            should_abort = True
                    if should_abort:
                        print("aborted!")
                        self.send_abort()
                        self.pc_stage = 0

                    else:
                        self.send_req(PRE_COMMIT, PRECOMMIT_STAGE)
                        c_t_o.reset()

                # coordinator PRE COMMIT STAGE
                if self.pc_stage == PRECOMMIT_STAGE:
                    # log precommit state?
                    self.send_req(COMMIT, COMMIT_STAGE)
                    c_t_o.reset()

                # coordinator COMMIT STAGE
                if self.pc_stage == COMMIT_STAGE:
                    self.commit(self.master_commands["commit"])
                    self.pc_stage = 0
        else:
            # NOT the coordinator
            if not p_t_o.waiting():
                if self.prev_stage == self.pc_stage:
                    do_stuff = 0
                else:
                    p_t_o.reset()



    def commit(self,request):
        print "I AM COMMITTING"
        c_array = request.split(" ")
        print c_array
        if c_array[0] == "add":
            self.songs[c_array[1]] = c_array[2]
        else:
            if c_array[1] in self.songs: del self.songs[c_array[1]]
        if self.is_coordinator():
            if self.m_client:
                self.m_client.client.send("ack commit" + "\n")
        self.master_commands= copy.deepcopy(BASE_STATE)


    def send_abort(self):
        for p_id in range(0,self.n):
            if p_id != self.id:
                try:
                    Connection_Client(GPORT+ p_id, self.id, SHOULD_ABORT).run()
                except:
                    continue
        self.abort()

    def abort(self):
        print "I AM ABORTING"
        if self.is_coordinator():
            if self.m_client:
                self.m_client.client.send("ack abort" + "\n")
        self.master_commands= copy.deepcopy(BASE_STATE)

    # coordinator
    def send_req(self, message, stage, request=""):
        self.pc_stage = stage
        self.states = {}
        if len(request) > 0:
            message += " "+request
        for p_id in self.up_set:
            if p_id != self.id:
                try:
                    print "sending requests: "+str(p_id)
                    Connection_Client(GPORT+ p_id, self.id, message).run()
                except:
                    self.states[self.id] = False
                    continue

    # process
    def recieve_request(self, request):
        data = parse_process_message(request)
        message = ""
        should_crash_after_send = False

        if data["command"] == VOTE_REQ:
            self.master_commands["commit"] = data["message"].strip()
            message = VOTE_YES
            self.pc_stage = PRECOMMIT_STAGE
            print self.master_commands

            if self.master_commands["vote"] == True:
                message = SHOULD_ABORT
                self.pc_stage = 0
                self.abort()

            if self.master_commands["crashAfterVote"]:
                should_crash_after_send = True

        elif data["command"] == PRE_COMMIT:
            message = PRE_COMMIT_ACK
            self.pc_stage = COMMIT_STAGE
            if self.master_commands["crashAfterAck"]:
                should_crash_after_send = True

        elif data["command"] == COMMIT:
            self.commit(self.master_commands["commit"])
            self.pc_stage = 0
            # no need to send message since it's commit, so we return
            return

        try:
            Connection_Client(GPORT+ self.coordinator, self.id, message).run()
        except:
            donothing

        if should_crash_after_send:
            self.crash()

class DTLog():
    def __init__(self, process):
        self.process = process
        self.iteration = None

    def log(phase, state):
        print "need to log state by phase"

if __name__ == "__main__":
    p_id = int(sys.argv[1])
    p_n = int(sys.argv[2])
    p_master_port = int(sys.argv[3])
    process = Process(p_id, p_n, GPORT + p_id, p_master_port)
    servers = [Server(p_master_port, process), Server(GPORT + p_id, process)]
    for s in servers:
        s.start()
        thread_list.append(s)
    # for s in servers:
    #     s.join()
    # while(True):
    #     time.sleep(1)
    #     print("awake")
