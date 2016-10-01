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
TIMEOUT = 0.3

# PROCCESS MESSAGE TYPES:
ELECTION_ACK = "ELECTION_ACK"
UR_ELECTED = "UR_ELECTED"

class TimeOut():
    def __init__(self):
        self.end = time.time()+TIMEOUT
    def waiting(self):
        return time.time() > self.end
    def reset(self):
        self.end = time.time()+TIMEOUT

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
        self.process.t_o.reset();
        while running:
            if self.port >= 20000 and self.port <= 29999 and not self.process.isCoordinator() and (not self.process.coordinator or not self.process.t_o.waiting()):
                self.process.elect_coordinator()
            inputready,outputready,exceptready = select.select(input,[],[])
            for s in inputready:

                if s == self.server:
                    # handle the server socket
                    c = Client(self.server.accept(), self.process, self.port)
                    c.start()
                    self.threads.append(c)

                elif s == sys.stdin:
                    # handle standard input
                    junk = sys.stdin.readline()
                    print "you printed "+junk +" for "+str(20000+(self.port-1)%2)
                    # Connection_Client(20000+(self.port-1)%2, self.process.id, junk).run()
                    running = 0

        # close all threads
        self.server.close()
        for c in self.threads:
            c.join()

    def crash(self):
        for c in self.threads:
            c.join()

class Client(threading.Thread):
    def __init__(self,(client,address), process, port):
        threading.Thread.__init__(self)
        self.client = client
        self.address = address
        self.port = port
        self.size = 1024
        self.process = process

        print 'Got connection from', address, ':', port

    def run(self):
        running = 1
        while running:
            # using try except block because second ask for data the sever has closed the connection
            try:
                data = self.client.recv(self.size)
                if data:
                    # check coordinator heartbeat
                    if (self.port - 20000) == process.coordinator:
                        self.process.t_o.reset()
                    self.client.send(data)
                    print data, ':', self.port
                    process_lock.acquire()
                    self.process.process_command(data, self.port)
                    process_lock.release()
                else:
                    self.client.close()
                    running = 0
                return data
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
        self.t_o = TimeOut()
        self.songs = {}
        self.state = None
        self.up_set = set([x for x in range(0,n)])
        self.master_commands = {}

    def isCoordinator(self):
        return self.coordinator == self.id

    def process_master_command(self, command, port):
        command_array = command.split(" ")
        command_key = command_array[0]
        if(port == self.m_port): #PROCESS MASTER COMMANDS:
            print 'Command key is:' , command_key.replace('\n', '')
            if command_key.replace('\n', '') == "crash":
                print "Crashing!!!!!"
                self.crash()
            else:
                self.master_commands[command_key] = command

    def process_p_request(self,request):
        print "the request: "+request

    def crash(self):
        subprocess.Popen(['./kill_script', str(self.m_port)], stdout=open('/dev/null'), stderr=open('/dev/null'))

    def elect_coordinator(self):
        print "ELECTING NEW COORDINATOR"
        self.up_set.discard(self.coordinator)
        n_c_pid = min(self.up_set)
        if n_c_pid == self.id:
            print "I AM THE COORDINATOR"
            self.coordinator = self.id
        else:
            # ping coordinator
            Connection_Client(20000+n_c_pid, n_c_pid, UR_ELECTED).run()

if __name__ == "__main__":
    p_id = int(sys.argv[1])
    p_n = int(sys.argv[2])
    p_master_port = int(sys.argv[3])
    process = Process(p_id, p_n, 20000 + p_id, p_master_port)
    servers = [Server(p_master_port, process), Server(20000 + p_id, process)]
    for s in servers:
        s.start()
        thread_list.append(s)
    # for s in servers:
    #     s.join()
    # while(True):
    #     time.sleep(1)
    #     print("awake")
