import socket
import time
import json
from helper import *
import sys

host = '127.0.0.1'
port = 6662



TIMEOUT = 0.2
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.settimeout(None)


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

cid = int(sys.argv[1])
for i in range(5):
    finished = False
    message = getRequestObj(cid, '{} hello {}'.format(cid, i), i)
    message = json.dumps(message)
    while not finished:
        print('try resend')
        s.sendto(message.encode(), (host, port))
        data, addr = s.recvfrom(1024)
        print(data.decode())
        try:
            reply_obj = json.loads(data.decode())
        except:
            pass
        if reply_obj['message_type'] == 'Reply' and reply_obj['client_id'] == cid and reply_obj['seq_num'] == i:
            finished = True

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
