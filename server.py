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

process_lock = threading.Lock()
thread_list = []

# Timeout value
TIMEOUT = 5
HB_TIMEOUT = 0.2 * TIMEOUT

TIMEOUT = 5
HB_TIMEOUT = 0.05 * TIMEOUT

GPORT = 20000

# PROCCESS MESSAGE TYPES:
I_AM_COORDINATOR = "I_AM_COORDINATOR"
ELECTION_ACK = "ELECTION_ACK"
UR_ELECTED = "UR_ELECTED"
HEARTBEAT = "HEARTBEAT"
HEARTBEAT_ACK = "HEARTBEAT_ACK"

def check_port(port):
    return port >= GPORT and port < (GPORT+10000)

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
        pto = TimeOut
        cto = 
        while running:
            if check_port(self.port):
                if not self.process.isCoordinator():
                    if not t_o.waiting():
                        self.process.elect_coordinator()
                        continue
                    if not self.process.state != 0 and cto.waiting():
                        self.process.do3pc()
                        cto.reset()
                else:
                    if not hb_t_0.waiting():
                        for p_id in range(0,self.process.n):
                            if p_id != self.process.id:
                                try:
                                    Connection_Client(GPORT+p_id, self.process.id, HEARTBEAT).run()
                                except:
                                    continue
                        hb_t_0.reset()
                    if not self.process.state != 0 and pto.waiting():
                        self.process.do3pc()
                        pto.reset()

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
                else:
                    self.client.close()
                    running = 0
            except:
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
            self.message += "_id="+str(self.p_id)
        s.send(self.message)
        s.close()

    def crash(self):
        sys.exit(0)


class Process():
    # We should probably make this a monitor.
    def __init__(self, p_id, n, self_port, master_port):
        self.coordinator = None
        self.id = p_id
        self.n = n
        self.port = self_port
        self.m_port = master_port
        self.m_client = None
        self.songs = {}
        self.up_set = set([x for x in range(n)])
        self.vote_set = []

        self.master_commands = []
        self.state = 0

        #Test cases:
        self.vote_no = False
        self.vote_crash = False
        self.ack_crash = False
        self.vote_req_crash = []
        self.precom_crash = []
        self.com_crash = []

    def isCoordinator(self):
        return self.coordinator == self.id

    def process_command(self, command, port):
        c_array = command.replace("\n", "").split(" ")
        if(port == self.m_port): #PROCESS MASTER COMMANDS:
            print 'Master command is: ' + (" ".join(c_array))
            #Crash Command:
            if c_array[0] == "crash":
                print (" ".join(c_array))
                self.crash()

            # FLag Commands:
            elif c_array[0] == "vote" and c_array[1] == "NO":
                print(" ".join(c_array))
                self.vote_no = True
            elif c_array[0] == "crashAfterVote":
                print (" ".join(c_array))
                self.vote_crash = True
            elif c_array[0] == "crashAfterAck":
                print(" ".join(c_array))
                self.ack_crash == True
            elif c_array[0] == "crashVoteReq":
                print(" ".join(c_array))
                for each in c_array[1:]:
                    self.vote_req_crash.append(int(each))
            elif c_array[0] == "crashPartialPreCommit":
                print(" ".join(c_array))
                for each in c_array[1:]:
                    self.precom_crash.append(int(each))
            elif c_array[0] == "crashPartialCommit":
                print(" ".join(c_array))
                for each in c_array[1:]:
                    self.com_crash.append(int(each))

            # Get Command
            elif c_array[0] == "get":
                try:
                    to_return = "resp " + str(self.songs[c_array[1]])
                except:
                    to_return = "resp NONE"
                return to_return

            # 3PC commands:
            elif c_array[0] == "add"  and self.coordinator == self.id:
                self.master_commands = [x for x in c_array]
                self.state = 1
                self.do3pc()
                
                # self.songs[c_array[1]] = c_array[2]
                # return "ack commit"
            elif c_array[0] == "delete" and self.coordinator == self.id:
                # self.songs = {key: value for key, value in self.songs.items() if key != c_array[1]}
                # return "ack commit"
                self.master_commands = [x for x in c_array]
                self.state = 1
                self.do3pc()
        else:
            split = command.split("_")
            if split[0] == HEARTBEAT:
                p_id = int(split[1].split("=")[1])
                if not self.process.coordinator:
                    print "ALERT - NO COORDINATOR SO SETTING COORDINATOR ",p_id
                    self.process.coordinator = p_id
                if p_id == self.process.coordinator:
                    self.server_t_o.reset()
            elif split[0] == "VOTE_REQ":
                self_state = 1
                self.do3pc()
            elif split[0] == "PRECOMMIT":
                self_state = 2
                self.do3pc()
            elif split[0] == "add":
                self.songs[split[1]] = split[2].split("=")[0]
                self.state = 0
            elif split[0] == "delete":
                self.songs = {key: value for key, value in self.songs.items() if key != split[1].split("=")[0]}
                self.state = 0
            print 'Process ', port, ' wants your attention'
        # return 'wtf?'
        return None

    def do3pc(self):
        if(self.isCoordinator):
            if self.state == 1:
                for p_id in self.up_set:
                    if p_id != self.id:
                        try:
                            Connection_Client(GPORT + p_id, p_id, "VOTE_REQ").run()
                        except:
                            continue
                self.state += 1
            elif self.state == 2:
                self.vote_set.append(self.id)
                if(set(self.vote_set) == set(self.up_set)):
                    for p_id in self.up_set:
                    if p_id != self.id:
                        try:
                            Connection_Client(GPORT + p_id, p_id, "PRECOMMIT").run()
                        except:
                            continue
                self.vote_set = []
                self.state += 1
            elif self.state == 3
                self.vote_set.append(self.id)
                self.up_set = sorted([x for x in self.vote_set])

                if master_commands[0] == "add"
                    self.songs[master_commands[1]] = master_commands[2]
                elif master_commands[0] == "delete" and self.coordinator == self.id:
                    self.songs = {key: value for key, value in self.songs.items() if key != master_commands[1]}

                for p_id in self.up_set:
                    if p_id != self.id:
                        try:
                            Connection_Client(GPORT + p_id, p_id, "_".join(self.master_commands)).run()
                        except:
                            continue
            self.state = 0
        else:
            if self.state == 1:
                try:
                    Connection_Client(GPORT + self.coordinator, p_id, "yes").run()
                except:
                    continue
                self_state = -1
            elif self.state == 2:
                try:
                    Connection_Client(GPORT + self.coordinator, p_id, "ack").run()
                except:
                    continue
                self_state = -1
            elif self_state == -1:
                print "Timed out, Run Termination!!"
                self.state = 0

    def crash(self):
        subprocess.Popen(['./kill_script', str(self.m_port)], stdout=open('/dev/null'), stderr=open('/dev/null'))

    def elect_coordinator(self):
        self.up_set.discard(self.coordinator)
        self.coordinator = min(self.up_set)
        print "ELECTING NEW COORDINATOR: "+str(self.coordinator)
        if self.coordinator == self.id:
            print "I AM THE COORDINATOR"
            self.coordinator = self.id
            self.m_client.client.send("coordinator " + str(self.id) + "\n")
            for p_id in self.up_set:
                if p_id != self.id:
                    try:
                        Connection_Client(GPORT+ p_id, p_id, I_AM_COORDINATOR).run()
                    except:
                        continue
        else:
            # ping coordinator
            try:
                Connection_Client(GPORT+n_c_pid, n_c_pid, UR_ELECTED).run()
            except:
                self.elect_coordinator()

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
