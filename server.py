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
TIMEOUT = 0.7
HB_TIMEOUT = 0.2*TIMEOUT

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
        while running:
            if check_port(self.port):
                if not self.process.isCoordinator():
                    if not t_o.waiting():
                        print "timeout"
                        self.process.elect_coordinator()
                        continue
                else:
                    if not hb_t_0.waiting():
                        for p_id in range(0,self.process.n):
                            if p_id != self.process.id:
                                try:
                                    Connection_Client(GPORT+p_id, self.process.id, HEARTBEAT).run()
                                except:
                                    continue
                        hb_t_0.reset()
            inputready,outputready,exceptready = select.select(input,[],[])
            for s in inputready:
                if s == self.server:
                    # handle the server socket
                    c = Client(self.server.accept(), self.process, self.port, t_o)
                    c.start()
                    self.threads.append(c)

                # elif s == sys.stdin:
                #     # handle standard input
                #     junk = sys.stdin.readline()
                #     print "you printed "+junk +" for "+str(GPORT+(self.port-1)%2)
                #     # Connection_Client(GPORT+(self.port-1)%2, self.process.id, junk).run()
                #     running = 0

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
            # using try except block because second ask for data the sever has closed the connection
            # try:
                data = self.client.recv(self.size)
                if data:
                    self.server_t_o.reset()
                    self.client.send(data)
                    # print data, ':', self.port
                    process_lock.acquire()
                    self.process.process_command(data, self.port)
                    process_lock.release()

                    if check_port(self.port):
                        split = data.split("_")
                        if len(split) == 2:
                            p_id = int(split[1].split("=")[1])
                            self.server_t_o.reset()
                else:
                    self.client.close()
                    running = 0
                return data
            # except :
                # running = 0

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
        self.songs = {}
        self.state = None
        self.up_set = set([x for x in range(0,n)])
        self.master_commands = {}

    def isCoordinator(self):
        return self.coordinator == self.id

    def process_command(self, command, port):
        command_array = command.split(" ")
        command_key = command_array[0]
        if(port == self.m_port): #PROCESS MASTER COMMANDS:
            print 'Command key is:' , command_key.replace('\n', '')
            if command_key.replace('\n', '') == "crash":
                print "Crashing!!!!!"
                self.crash()
            else:
                self.master_commands[command_key] = command

    def crash(self):
        subprocess.Popen(['./kill_script', str(self.m_port)], stdout=open('/dev/null'), stderr=open('/dev/null'))

    def elect_coordinator(self):
        self.up_set.discard(self.coordinator)
        self.coordinator = min(self.up_set)
        print "ELECTING NEW COORDINATOR: "+str(self.coordinator)
        if self.coordinator == self.id:
            print "I AM THE COORDINATOR"
            self.coordinator = self.id
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
