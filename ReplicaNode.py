import socket
import sys
import json
import threading
import time
from helper import *


class Replica:
    CLIENT_REPLY_MSG = 'message {} is set at sequence {}'


    def __init__(self, config_file, replicas_file, id):
        with open(config_file, 'r') as jsonfile:
            self.config = json.load(jsonfile)
        with open(replicas_file, 'r') as jsonfile:
            self.replicas = tuple(json.load(jsonfile))
        self.id = id
        # initial view
        self.view = 0
        # last_accepted = last accepted 'Command' message object
        self.last_accepted = None
        # clients = {
        #     id: seq_num
        # }
        self.clients = {}
        # leader use this dictionary find connection to reply a client
        self.clients_connection = {}
        # votes[i] = number of votes of the slot i command
        self.votes = []
        # operations[i] = agreed command at slot i
        self.operations = []


        self.host = self.replicas[self.id]['host']
        self.port = self.replicas[self.id]['port']
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
        message = get_message(connection)
        msg_obj = json.loads(message)
        self.handle_msg(msg_obj, connection)

    def handle_msg(self, msg_obj, connection):
        message_type = msg_obj['message_type']
        if message_type == 'Request':
            # only the leader receives Request message from client
            # i think i am the leader
            if self.view == self.id:
                client_id = msg_obj['client_id']
                client_seq = msg_obj['client_seq']
                if self.is_client_valid(client_id, client_seq):
                    self.clients[client_id] = client_seq
                    seq = len(self.operations)
                    self.votes.append(set())
                    self.votes[seq].add(self.id)
                    command = msg_obj['command']
                    command_msg = getCommandMsg(client_id, client_seq, self.id, seq, command)
                    self.last_accepted = json.loads(command_msg)
                    self.broadcast_msg(command_msg)
                    self.clients_connection[client_id] = connection
                    #send_message("OK", None, self.clients_connection[client_id])
        else:
            connection.close()
            if message_type == 'Command':
                leader_id = msg_obj['leader_id']
                if leader_id < self.view:
                    pass
                else:
                    client_id = msg_obj['client_id']
                    client_seq = msg_obj['client_seq']
                    seq_num = msg_obj['seq_num']
                    if self.is_client_valid(client_id, client_seq) and self.is_seq_num_valid(seq_num):
                        self.votes.append(set())
                        self.votes[seq_num].add(leader_id)
                        self.votes[seq_num].add(self.id)
                        self.clients[client_id] = client_seq
                        self.last_accepted = msg_obj
                        # broadcast accept message
                        accept_msg = getAcceptMsg(self.id, leader_id, client_id, client_seq, seq_num)
                        self.broadcast_msg(accept_msg)
            elif message_type == 'Accept':
                leader_id = msg_obj['leader_id']
                if leader_id < self.view:
                    pass
                else:
                    client_id = msg_obj['client_id']
                    client_seq = msg_obj['client_seq']
                    seq_num = msg_obj['seq_num']
                    voter_id = msg_obj['replica_id']
                    print('receive accept message ' + str(voter_id))
                    if self.clients[client_id] == client_seq and seq_num <= len(self.operations):
                        self.votes[seq_num].add(voter_id)

                    print('current voters are')
                    print('\t' + str(self.votes[seq_num]))
                    if len(self.votes[seq_num]) > self.config['f']:
                        if len(self.operations) <= seq_num:
                            self.operations.append(self.last_accepted['command'])
                            if self.id == self.view:
                                print('send message back to client ????????????? ')
                                client_connection = self.clients_connection[client_id]
                                print(
                                    send_message(self.CLIENT_REPLY_MSG.format(self.operations[seq_num], seq_num), None,
                                                 s=client_connection))
                        self.last_accepted = None
                        # I am the leader

            else:
                print("WTF" + "=" * 15)
                print(msg_obj['message_type'])

    def print_status(self):
        print(self.socket)

    def is_client_valid(self, client_id, client_seq):
        return client_id not in self.clients or client_seq > self.clients[client_id]

    def is_seq_num_valid(self, seq_num):
        return seq_num >= len(self.operations)

    def broadcast_msg(self, msg_str):
        for i in range(len(self.replicas)):
            if not i == self.id:
                host = self.replicas[i]['host']
                port = self.replicas[i]['port']
                print((host, port))
                # TODO: add retry
                send_message(msg_str, (host, port))

if __name__ == '__main__':
    id = int(sys.argv[1])
    replica = Replica('config1.json', 'replicas.json', id)
    replica.print_status()
    # replica.connect_other_replicas()
    replica.listen()