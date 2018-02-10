import socket
import sys
import json
import configparser
import threading


class Replica:
    BUFFER_SIZE = 4096

    def __init__(self, config_file, replicas_file, id):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        with open(replicas_file, 'r') as jsonfile:
            self.replicas = json.load(jsonfile)
        self.id = id
        self.host = self.replicas[self.id]['host']
        self.port = self.replicas[self.id]['port']
        del self.replicas[self.id]
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))

    def listen(self):
        self.socket.listen(len(self.replicas) + 5)

        while True:
            connection, address = self.socket.accept()
            thread = threading.Thread(target=self.__listen_to_connection, args=(connection, address))
            thread.start()

    def __listen_to_connection(self, connection, address):
        message = ''
        while True:
            data = connection.recv(self.BUFFER_SIZE)
            message += data.decode('utf-8')
            if (len(data) != self.BUFFER_SIZE):
                break
        self.handle_msg(message, connection)

    def handle_msg(self, message, connection):
        print(message)
        connection.send(str(len(message)).encode('utf-8'))
        connection.close()

    def print_status(self):
        print(self.socket)



if __name__ == '__main__':
    id = int(sys.argv[1])
    replica = Replica('config1.ini', 'replicas.json', id)
    replica.print_status()
    replica.listen()
