import socket
import sys
import json
import configparser
import threading
import time
from helper import *


class Replica:

    def __init__(self, config_file, replicas_file, id):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        with open(replicas_file, 'r') as jsonfile:
            self.replicas = tuple(json.load(jsonfile))
        self.id = id
        # initial view
        self.view = 0
        self.is_leader = (self.id == 0)
        self.operations = []
        self.host = self.replicas[self.id]['host']
        self.port = self.replicas[self.id]['port']
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))

    def connect_other_replicas(self):
        time.sleep(5)
        for i in range(len(self.replicas)):
            if i == self.id:
                pass
            else:
                host = self.replicas[i]['host']
                port = self.replicas[i]['port']
                print(port)
                self.socket.connect((host, port))
                print("connect to {}: {}".format(host, port))


    def listen(self):
        self.socket.listen(len(self.replicas) + 5)

        while True:
            connection, address = self.socket.accept()
            thread = threading.Thread(target=self.__listen_to_connection, args=(connection, address))
            thread.start()

    def __listen_to_connection(self, connection, address):
        message = receive_message(connection)
        msg_obj = json.loads(message)
        print(msg_obj)
        self.handle_msg(msg_obj, connection)

    def handle_msg(self, msg_obj, connection):
        connection.send(str(len(msg_obj)).encode('utf-8'))
        connection.close()
        if self.is_leader:
            for i in range(len(self.replicas)):
                if i == self.id:
                    pass
                else:
                    host = self.replicas[i]['host']
                    port = self.replicas[i]['port']
                    print((host, port))
                    msg_obj['leader_id'] = self.id
                    msg_str = json.dumps(msg_obj)
                    send_message(msg_str, (host, port))

    def print_status(self):
        print(self.is_leader)
        print(self.socket)



if __name__ == '__main__':
    id = int(sys.argv[1])
    replica = Replica('config1.ini', 'replicas.json', id)
    replica.print_status()
    # replica.connect_other_replicas()
    replica.listen()