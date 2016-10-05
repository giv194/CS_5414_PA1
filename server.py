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
import ast

process_lock = threading.Lock()
thread_list = []

BASE_STATE = {
    "vote": False,
    "crashAfterVote": False,
    "crashAfterAck": False,
    "crashVoteREQ": False,
    "crashPartialCommit": False,
    "crashPartialPreCommit": False,
    "commit": ""
}

# Timeout value
TIMEOUT = 1
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
PRE_COMMIT = "PRE_COMMIT2"
PRE_COMMIT_ACK = "PRE_COMMIT_ACK3"
COMMIT = "COMMIT3"
STABLE = "STABLE"
ABORT = "ABORT"
GET_TABLES = "GET_TABLES"
SET_TABLES = "SET_TABLES"

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
        self.host = 'localhost'
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
        p_t_o = TimeOut(TIMEOUT)
        while running:
            if check_port(self.port):
                if self.process.pc_stage == 0:
                    c_t_o.reset()
                    p_t_o.reset()
                    if not self.process.is_coordinator():
                        if not t_o.waiting():
                            print "timeout"
                            #check DT_log if you're the first to wake up:
                            log_data = self.process.read_log()
                            if log_data != None:
                                print "RESTORING DATA!"
                                self.process.songs = ast.literal_eval(log_data["db"])
                                self.process.dt_index = log_data["dt_index"]
                                self.process.up_set = set([x for x in log_data["up_set"]])
                                if len(log_data["up_set"]) == 1 or self.process.coordinator != None:
                                    self.process.elect_coordinator()
                                else:
                                    for p_id in self.process.up_set:
                                        if p_id != self.process.id and self.process.master_commands["commit"] != "":
                                            try:
                                                #FINISH THE COMMAND BUFFER!!!
                                                print "SEND COMMAND BUFFER: ", str(self.process.master_commands["commit"]) + " COMMAND"
                                                Connection_Client(GPORT+p_id, self.process.id, str(self.process.master_commands["commit"]) + " COMMAND").run()
                                            except:
                                                donothing = 0
                                if self.process.coordinator == None:
                                    self.process.m_client.client.send("coordinator " + str(self.process.id) + "\n")
                            else:
                                self.process.elect_coordinator()
                            t_o.reset()
                    else:
                        if not hb_t_0.waiting():
                            for p_id in range(0,self.process.n):
                                if p_id != self.process.id:
                                    try:
                                        Connection_Client(GPORT+p_id, self.process.id, HEARTBEAT + "_" +str(self.process.dt_index)).run()
                                        self.process.m_client.client.send("coordinator " + str(self.process.id) + "\n")
                                    except:
                                        donothing = 0
                            hb_t_0.reset()
                            if self.process.master_commands["commit"] != "" and self.process.pc_stage == 0:
                                self.process.process_command(self.process.master_commands["commit"], self.process.m_port)
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

                # elif s == sys.stdin:
                    # handle standard input
                    # junk = sys.stdin.readline()
                    # print "Shutting down server %d ..."%(self.port%20000)
                    # running = 0
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
            if self.port == self.process.m_port:
                process_lock.acquire()
                process_lock.release()
            try:
                data = self.client.recv(self.size)
                if data:
                    #echo server:
                    #self.client.send(data)
                    # print data, ':', self.port
                    to_return = self.process.process_command(data, self.port)
                    if to_return != None:
                        self.client.send(str(to_return) + "\n")

                    if check_port(self.port):
                        if HEARTBEAT in data:
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
        self.master_commands = copy.deepcopy(BASE_STATE)

    def is_coordinator(self):
        return self.coordinator == self.id

    def process_command(self, command_string, port):
        command_string = command_string.replace("\n", "")
        c_array = command_string.split(" ")
        if(port == self.m_port or "COMMAND" in c_array): #PROCESS MASTER COMMANDS:
            if "COMMAND" in c_array and self.is_coordinator():
                self.up_set.add(int(c_array[4].split("=")[1]))

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
            elif command == "crashVoteREQ":
                print(" ".join(c_array))
                self.master_commands[command] = [int(i) for i in c_array[1:] if i]
                print self.master_commands[command]
            elif command == "crashPartialPreCommit":
                print(" ".join(c_array))
                self.master_commands[command] = [int(i) for i in c_array[1:] if i]
            elif command == "crashPartialCommit":
                print(" ".join(c_array))
                self.master_commands[command] = [int(i) for i in c_array[1:] if i]

            # Get Command
            elif command == "get":
                process_lock.acquire()
                try:
                    to_return = "resp " + str(self.songs[c_array[1]])
                except:
                    to_return = "resp NONE"
                process_lock.release()
                return to_return

            # 3PC commands:
            elif command == "add":
                self.master_commands["commit"] = command_string
                if self.coordinator == self.id:
                    process_lock.acquire()
                    # self.songs[c_array[1]] = c_array[2]
                    print c_array
                    # increase dt_index for the master
                    self.dt_index += 1
                    self.send_req(VOTE_REQ, VOTE_STAGE, command_string.replace("COMMAND ", ""))
                    return None
            elif command == "delete":
                self.master_commands["commit"] = command_string
                if self.coordinator == self.id:
                    process_lock.acquire()
                    # self.songs = {key: value for key, value in self.songs.items() if key != c_array[1]}
                    print c_array[1:]
                    # increase dt_index for the master
                    self.dt_index += 1
                    self.send_req(VOTE_REQ, VOTE_STAGE, command_string.replace("COMMAND ", ""))
                    return None

            if "COMMAND" in c_array:
                split = command_string.split(" COMMAND")
                ids = []
                for each in split:
                    if "id=" in each:
                        print each.split("=")[1]
                        ids.append(int(each.split("=")[1]))
                print "UP NODES: ", ids
                for up in self.up_set:
                    if up not in ids and up != self.id:
                        print "NODEs ARE NOT UP: ", up
                        return None
                print "ALL NODES ARE UP"
                self.elect_coordinator()

        else:
            # commands from process to coordinator
            if VOTE_YES in c_array[0]:
                data = parse_process_message(command_string)
                self.states[data["id"]] = True
                print self.states
            if SHOULD_ABORT in c_array[0]:
                data = parse_process_message(command_string)
                self.states[data["id"]] = False
                print self.states

            if PRE_COMMIT_ACK in c_array[0]:
                data = parse_process_message(command_string)
                self.states[data["id"]] = True

            if GET_TABLES in c_array[0]:
                data = parse_process_message(command_string)
                print "GET_TABLES: ", data
                message = "SET_TABLES " + str(self.songs)
                #update up_set:
                self.up_set.add(data["id"])
                Connection_Client(GPORT + data["id"], self.id, message).run()

            if "STATE_RETURN" in c_array[0]:
                print "STATE_RETURN:", c_array
                state = ast.literal_eval(c_array[1])
                stage = int(c_array[2])
                dt_index = int(c_array[3])
                p_id = int(c_array[4].split("=")[1])

                self.states[p_id] = True
                print "MY PREV STAGE", self.prev_stage

                if dt_index < self.dt_index:
                    # ABORT:
                    print "TR1 TIMEOUT ABORT"
                    self.prev_stage = 0
                    self.send_abort()

                if state["vote"] == True:
                    # ABORT:
                    print "TR1 VOTE NO ABORT"
                    self.prev_stage = 0
                    self.send_abort()

                if stage == 2 and self.pc_stage != 2:
                    # PRECOMMIT TR4
                    print "PRECOMMIT TR4"
                    # self.send_req(PRE_COMMIT, PRECOMMIT_STAGE)
                    self.pc_stage = 2

                if stage == 3:
                    # COMMIT TR2
                    print "COMMIT TR2"
                    self.pc_stage = 2

            # commands from coordinator to process
            if VOTE_REQ in c_array[0]:
                process_lock.acquire()
                self.recieve_request(command_string)

            elif PRE_COMMIT in c_array[0]:
                self.recieve_request(command_string)

            elif COMMIT in c_array[0]:
                self.recieve_request(command_string)

            elif SET_TABLES in c_array[0]:
                print "TABLES FROM MASTER:"
                print c_array
                self.recieve_request(command_string)

            elif HEARTBEAT in c_array[0]:
                p_id = int(c_array[1].split("=")[1])
                print 'Process ', p_id, ' wants your attention'
                if p_id != self.coordinator:
                    print "SETTING COORDINATOR ", p_id

                    log_data = self.read_log()
                    if log_data != None:
                        self.dt_index = log_data["dt_index"]
                        if (self.coordinator == None):
                            #pickup tables from the log
                            self.songs = ast.literal_eval(log_data["db"])
                    self.coordinator = p_id
                    if (self.dt_index < int(c_array[0].split("_")[1])) or self.pc_stage != 0 or (log_data != None and p_id not in log_data["up_set"]):
                        print "My dt_ind", self.dt_index, ":", c_array[0].split("_")[1]
                        print "GETTING TABLES"
                        self.dt_index = int(c_array[0].split("_")[1])
                        self.recieve_request(GET_TABLES)
            
            elif "STATE_REQ" in c_array[0]:
                print "STATE_REQ recieved!"
                self.recieve_request(command_string) 
        # return 'wtf?'
        # return "ack abort"
        return None

    # def create_request(self, c_array):
    #     request = "commit=" + c_array[0] + " name=" + c_array[1]
    #     if len(c_array) == 3:
    #         request +=  " url=" + c_array[2]
    #     print request
    #     return request

    def crash(self):
        subprocess.Popen(['./kill_script', str(self.m_port)], stdout=open('/dev/null'), stderr=open('/dev/null'))
        exit(0)


    def elect_coordinator(self):
        if self.coordinator == None:
            self.coordinator = 0
        else:
            self.coordinator = (self.coordinator + 1) % self.n
        print "ELECTING NEW COORDINATOR: "+str(self.coordinator)
        if self.coordinator == self.id:
            print "I AM THE COORDINATOR"
            if self.m_client:
                self.m_client.client.send("coordinator " + str(self.id) + "\n")
            if self.pc_stage == -1:
                self.termination()

    # 3PC methods
    def do_3pc(self, p_t_o, c_t_o):
        # check if coordinator
        # print "PREVIOUS STAGE", self.prev_stage
        if self.is_coordinator():
            if not c_t_o.waiting():
                # coordinator VOTE REQ STAGE
                if self.pc_stage == VOTE_STAGE:
                    # increase dt_index for a participant

                    self.log(VOTE_REQ)
                    should_abort = False
                    if "vote" in self.master_commands:
                        should_abort = self.master_commands["vote"]

                    # update up_set
                    for p_id in range(self.n):
                        if p_id != self.coordinator and p_id not in self.states:
                            self.up_set.discard(p_id)

                    for p_id in self.up_set:
                        if p_id != self.coordinator and self.states[p_id] == False:
                            should_abort = True

                    # if self.pc_stage == 1 and self.prev_stage == 1:
                    #     "IS IT IT?"
                    #     should_abort = True

                    if should_abort:
                        print("aborted!")
                        self.pc_stage = 0
                        self.send_abort()

                    else:
                        self.send_req(PRE_COMMIT, PRECOMMIT_STAGE)
                        c_t_o.reset()
                    return

                # coordinator PRE COMMIT STAGE
                if self.pc_stage == PRECOMMIT_STAGE:
                    for p_id in range(self.n):
                        if p_id != self.coordinator and p_id not in self.states:
                            self.up_set.discard(p_id)
                            print "DISCART FROM PRECOMMIT_STAGE", p_id
                    self.log(PRE_COMMIT)
                    # log precommit state?
                    self.send_req(COMMIT, COMMIT_STAGE)
                    c_t_o.reset()
                    return

                # coordinator COMMIT STAGE
                if self.pc_stage == COMMIT_STAGE:
                    self.log(COMMIT)
                    self.commit(self.master_commands["commit"])
                    self.pc_stage = 0
                    self.prev_stage = 3

                if self.pc_stage == -1:
                    print "WAITING FOR STAGES"
                    temp = self.prev_stage
                    self.pc_stage = self.prev_stage
                    self.prev_stage = temp
                    print "NEXT STAGE WILL BE", self.pc_stage
                    c_t_o.reset()
                    return

        else:
            # NOT the coordinator
            if not p_t_o.waiting():
                if self.prev_stage == self.pc_stage:
                    print "WE TIMED OUT ON", self.pc_stage
                    self.termination()
                else:
                    self.prev_stage = self.pc_stage
                p_t_o.reset()


    def termination(self):
        print "WE ARE IN TERMINATION:"
        if not self.is_coordinator():
            self.up_set.discard(self.coordinator)
            self.elect_coordinator()
            if self.coordinator == self.id:
                for p_id in self.up_set:
                    if p_id != self.id:
                        try:
                            Connection_Client(GPORT+p_id, self.id, HEARTBEAT + "_0").run()
                        except:
                            donothing = 0
                print "SENDING STATE REQUEST"
                for p_id in self.up_set:
                    if p_id != self.id:
                        try:
                            Connection_Client(GPORT+p_id, self.id, "STATE_REQ " + str(self.dt_index)).run()
                        except:
                            donothing = 0
            self.pc_stage = -1
        else:
            print "SENDING STATE REQUEST"
            for p_id in self.up_set:
                if p_id != self.id:
                    try:
                        Connection_Client(GPORT+p_id, self.id, "STATE_REQ " + str(self.dt_index)).run()
                    except:
                        donothing = 0


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
        self.log(STABLE)
        self.master_commands= copy.deepcopy(BASE_STATE)
        process_lock.release()


    def send_abort(self):
        for p_id in range(0,self.n):
            if p_id != self.id:
                try:
                    Connection_Client(GPORT+ p_id, self.id, SHOULD_ABORT).run()
                except:
                    continue
        print "I'm ABORTING"
        self.abort()

    def abort(self):
        print "I AM ABORTING"
        if self.is_coordinator():
            if self.m_client:
                self.m_client.client.send("ack abort" + "\n")
        self.log(ABORT)
        self.master_commands = copy.deepcopy(BASE_STATE)
        process_lock.release()

    # coordinator
    def send_req(self, message, stage, request=""):
        self.prev_stage = self.pc_stage
        self.pc_stage = stage
        self.states = {}
        if len(request) > 0:
            message += " "+request
        pids = self.up_set
        should_crash_after_send = False

        print "doing send request ",stage," and message ", message
        print pids
        if self.pc_stage == VOTE_STAGE and self.master_commands['crashVoteREQ'] != False:
            pids = self.master_commands['crashVoteREQ']
            should_crash_after_send = True

        elif self.pc_stage == PRECOMMIT_STAGE and self.master_commands['crashPartialPreCommit'] != False:
            pids = self.master_commands['crashPartialPreCommit']
            should_crash_after_send = True

        elif self.pc_stage == COMMIT_STAGE and self.master_commands['crashPartialCommit'] != False:
            pids = self.master_commands['crashPartialCommit']
            should_crash_after_send = True

        for p_id in pids:
            if p_id != self.id:
                try:
                    print "sending requests: "+str(p_id)
                    Connection_Client(GPORT+ p_id, self.id, message).run()
                except:
                    continue

        if should_crash_after_send:
            print "CRASHING"
            self.crash()

    # process
    def recieve_request(self, request):
        data = parse_process_message(request)
        print "PROCESS DATA", data
        message = ""
        should_crash_after_send = False

        if data["command"] == SHOULD_ABORT:
            message = SHOULD_ABORT
            self.pc_stage = 0
            self.log(message)
            self.abort()

        elif data["command"] == VOTE_REQ:
            self.dt_index += 1
            self.master_commands["commit"] = data["message"].strip()
            message = VOTE_YES
            self.pc_stage = VOTE_STAGE
            self.log(message)
            print self.master_commands
            #Abort:
            if self.master_commands["vote"] == True:
                message = SHOULD_ABORT
                self.pc_stage = 0
                self.log(message)
                self.abort()

            if self.master_commands["crashAfterVote"]:
                should_crash_after_send = True

        elif data["command"] == PRE_COMMIT:
            message = PRE_COMMIT_ACK
            self.pc_stage = PRECOMMIT_STAGE
            self.log(message)
            if self.master_commands["crashAfterAck"]:
                should_crash_after_send = True

        elif data["command"] == COMMIT:
            message = COMMIT
            self.pc_stage = COMMIT_STAGE
            self.log(COMMIT)
            self.commit(self.master_commands["commit"])
            self.pc_stage = 0
            self.prev_stage = 0
            # no need to send message since it's commit, so we return
            return

        elif data["command"] == GET_TABLES:
            print "WE ARE READY TO ASK FOR TABLES"
            message = GET_TABLES

        elif data["command"] == SET_TABLES:
            print "SETTING TABLES", data
            self.songs = ast.literal_eval(data["message"].replace(" ", ""))
            self.pc_stage = 0
            self.log(STABLE)
            print self.songs

        elif data["command"] == "STATE_REQ":
            print "Responding with the state"
            message = "STATE_RETURN " + str(self.master_commands).replace(" ", "") + " " + str(self.prev_stage) + " " + str(self.dt_index)
            self.pc_stage = 0

        try:
            Connection_Client(GPORT + self.coordinator, self.id, message).run()
        except:
            donothing = 0 

        if should_crash_after_send:
            self.crash()

    def log(self, message):
        with open("DTLog_" + str(self.id) + ".txt", "w") as f:
            f.write(json.dumps({"dt_index": self.dt_index,"coordinator": self.coordinator, "message": message, "stage": self.pc_stage, "db": str(self.songs), "up_set": [x for x in self.up_set]}))

    def read_log(self):
        data = None
        try:
            with open('DTLog_' + str(self.id) + ".txt") as data_file:    
                data = json.load(data_file)
            return data
        except:
            return data


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
