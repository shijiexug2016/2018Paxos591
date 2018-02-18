import socket
import time
import json
from helper import *

host = 'localhost'
port = 6662

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host, port))
message = {}
message['message_type'] = 4
message['client_id'] = 0
message['client_seq'] = 0
message['command'] = 'hello world'
json_data = json.dumps(message)
s.send(json_data.encode())
print(s.recv(1024).decode())
s.close()