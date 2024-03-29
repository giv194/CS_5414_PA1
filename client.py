#!/usr/bin/env python

"""
An echo client that allows the user to send multiple lines to the server.
Entering a blank line will exit the client.
"""

import socket
import sys

host = 'localhost'
port = int(sys.argv[1])
size = 1024
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host,port))
sys.stdout.write('%')

while 1:
    data = s.recv(size)
    if data:
        sys.stdout.write(data)
        sys.stdout.write('\n%')
    # read from keyboard
    line = sys.stdin.readline()
    if line == '\n':
        break
    s.send(line.replace('\n', ''))
s.close()