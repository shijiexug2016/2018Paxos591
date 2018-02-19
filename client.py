import socket
import time
import json
from helper import *

host = 'localhost'
port = 6662

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host, port))
message = getRequestMsg(0, "hello world", 0)
s.send(message.encode(CODE_METHOD))
print(s.recv(1024).decode(CODE_METHOD))
s.close()
