import socket

s = socket.socket()

host = 'localhost'

port = 6662

s.connect((host, port))

s.send('question is'.encode())
print(s.recv(1024).decode())
s.close()