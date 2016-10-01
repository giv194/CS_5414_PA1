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

process_lock = threading.Lock()
thread_list = []


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
        while running:

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
            data = self.client.recv(self.size)
            if data:
                #echo server:
                #self.client.send(data)
                #print data, ':', self.port
                process_lock.acquire()
                to_return = self.process.process_command(data, self.port)
                if to_return != None:
                    self.client.send(to_return)
                process_lock.release()
            else:
                self.client.close()
                running = 0
    

class Connection_Client(threading.Thread):
    def __init__(self, port, p_id, message):
        self.port = port
        self.p_id = p_id
        slef.message = message

    def run(self):
        host = 'localhost'
        size = 1024
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, self_port))
        s.send(self.message)
        s.close()

    def crash(self):
        sys.exit(0)

class Process():
    # We should probably make this a monitor.
    def __init__(self, p_id, n, self_port, master_port):
        self.coordinator = 0
        self.id = p_id
        self.n = n
        self.port = self_port
        self.m_port = master_port
        self.songs = {}
        self.up_set = [x for x in range(n)]
        self.master_commands = {}

        #Test cases:
        self.vote_no = False
        self.vote_crash = False
        self.ack_crash = False
        self.vote_req_crash = []
        self.precom_crash = []
        self.com_crash = []

    def process_command(self, command, port):
        c_array = command.split(" ")

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
                print(" ".join(c_array))
                try:
                    to_return = "resp " + str(self.songs[c_array[1]])
                except:
                    to_return = "NONE"
                return to_return
            
            # 3PC commands:
            elif c_array[0] == "add"  and self.coordinator == self.id:
                self.songs[c_array[1]] = c_array[2]
            elif c_array[0] == "delete" and self.coordinator == self.id:
                self.songs = {key: value for key, value in self.songs.items() if key != c_array[1]}
        else:
            print 'Process ', port, ' wants your attention'
            print command_key
        return command


    def crash(self):
        subprocess.Popen(['./kill_script', str(self.m_port)], stdout=open('/dev/null'), stderr=open('/dev/null'))

if __name__ == "__main__":
    p_id = int(sys.argv[1])
    p_n = int(sys.argv[2])
    p_master_port = int(sys.argv[3])
    process = Process(p_id, p_n, 20000 + p_id, p_master_port)
    servers = [Server(p_master_port, process), Server(20000 + p_id, process)]
    for s in servers:
        s.start()
        thread_list.append(s)
    for s in servers:
        s.join()
