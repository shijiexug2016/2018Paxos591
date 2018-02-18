import socket
from enum import Enum

BUFFER_SIZE = 4096
class MessageType(Enum):
    IAmLeader = 0
    Decree = 1
    YouAreLeader = 2
    Accept = 3
    Request = 4

def send_message(message, address):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(address)
    s.send(message.encode())
    response = receive_message(s)
    s.close()
    return response

def receive_message(connection):
    message = ''
    while True:
        data = connection.recv(BUFFER_SIZE)
        message += data.decode('utf-8')
        if (len(data) != BUFFER_SIZE):
            break
    return message