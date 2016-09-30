#!/usr/bin/env python

"""
An echo server that uses threads to handle multiple clients at a time.
Entering any line of input at the terminal will exit the server.
"""

import select
import socket
import sys
import threading

class Server:
    def __init__(self, process, s_master_port):
        self.process = process
        self.host = ''
        self.port = s_master_port
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

            if self.id == 1:
                SendToProcess(10000).run("hello")

            inputready,outputready,exceptready = select.select(input,[],[])
            for s in inputready:

                if s == self.server:
                    # handle the server socket
                    c = Client(self.server.accept())
                    c.start()
                    self.threads.append(c)

                elif s == sys.stdin:
                    # handle standard input
                    junk = sys.stdin.readline()
                    running = 0

        # close all threads
        self.server.close()

class Client(threading.Thread):
    def __init__(self,(client,address)):
        threading.Thread.__init__(self)
        self.client = client
        self.address = address
        self.size = 1024
        print 'Got connection from', address

    def run(self):
        running = 1
        while running:
            data = self.client.recv(self.size)
            print data
            if data:
                self.client.send(data)
            else:
                self.client.close()
                running = 0

class SendToProcess(threading.Thread):
    def __init__(self,destPort):
        threading.Thread.__init__(self)
        self.port = destPort
        self.size = 1024

    def run(self, message):
        host = ''
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host,self.port))
        s.send(message)
        s.close()


if __name__ == "__main__":
    p_id = int(sys.argv[1])
    p_n = int(sys.argv[2])
    p_master_port = int(sys.argv[3])
    process = Process(p_id, p_n)
    s = Server(process, p_master_port)
    s.run()

class Process():
    # We should probably make this a monitor.
    def __init__(self, p_id, n):
        self.id = p_id
        self.n = n
        self.songs = {}
        self.processes = Set([])
        self.up_set = Set([])
        self.master_commands = {}

    def processMasterCommand(self, command):
        command_array = command.split(" ")
        command_key = command_array[0]
        if command_key == "crash":
            self.crash();
        else:
            self.master_commands[command_key] = command


    def crash():
        exit(0)
