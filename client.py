import socket
import time
import json
from helper import *

host = 'localhost'
port = 6666

message = 'hello from client'

TIMEOUT = 0.2
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.settimeout(TIMEOUT)


s.sendto(message.encode(), (host, port))


def my_recvfrom(attempt, retrys = 3):
    if attempt <= retrys:
        try:
            data, addr = s.recvfrom(1024)
            print(data.decode())
        except socket.timeout as e:
            print(e)
            attempt += 1
            s.settimeout(TIMEOUT * attempt)
            my_recvfrom(attempt, retrys)
    else:
        print('lalalala')


my_recvfrom(0)
#
# try:
#     data, addr = s.recvfrom(1024)
# except socket.timeout as e:
#     print(e)

# print(data.decode())

# for i in range(2):
#     message = getRequestMsg(0, "hello world", 0)
#     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     try:
#         s.connect((host, port))
#         s.send(message.encode(CODE_METHOD))
#         print(s.recv(1024).decode(CODE_METHOD))
#     except Exception as e:
#         print(e)
#     #
#     s.close()
