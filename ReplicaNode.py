import sys
import threading
from helper import *


class Replica:
    CLIENT_REPLY_MSG = 'message {} is set at sequence {}'


    def __init__(self, config_file, id):
        with open(config_file, 'r') as jsonfile:
            configs = json.load(jsonfile)
            self.f = configs['f']
            self.skip_slot = configs['skip_slot']
            self.message_loss = configs['message_loss']
            self.replicas = configs['replicas']
            self.replicas = [tuple(replica) for replica in self.replicas]
        # replica id
        self.id = id
        # initial view, view keeps increasing
        # leader_id = view % # of replicas
        self.view = 0
        # current sequence number
        self.seq_num = 0
        # Am I elected
        self.elected = self.view == self.id
        # last_accepted = last accepted 'Command' message object
        self.last_accepted = None
        # clients = {
        #     id: {
        #         'client_seq' : client_seq,
        #         'addr' : (host, port)
        #     }
        # }
        self.clients = {}
        # the current followers
        self.followers = set()
        # previous proposals
        self.prev_proposals = []
        # votes[i] = number of votes of the slot i command
        self.votes = []
        # operations[i] = agreed command at slot i
        self.operations = []

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.replicas[self.id])
        logger.info(READY_LOG.format(self.id, self.replicas[self.id]))

    def listen(self):

        while True:
            msg_byte, address = self.socket.recvfrom(BUFFER_SIZE)
            msg_obj = json.loads(msg_byte.decode(CODE_METHOD))
            if address in self.replicas:
                # replica message
                pass
            else:
                # client message
                self.handle_client(msg_obj, address)
                pass
            #thread = threading.Thread(target=self.__listen_to_connection, args=(connection, address))
            #thread.start()

    def broadcast_msg(self, msg_str, timeout_handler):
        for i in range(len(self.replicas)):
            if not i == self.id:
                addr = self.replicas[i]
                send_msg(self.socket, msg_str, addr, 0, timeout_handler)

    # def __listen_to_connection(self, connection, address):
    #     message = get_message(connection)
    #     msg_obj = json.loads(message)
    #     self.handle_msg(msg_obj, connection)
    def handle_client(self, msg_obj, address):
        # I am the leader
        if self.I_am_the_leader():
            if self.elected:
                # quorums have learned previous command
                if self.seq_num == len(self.operations):
                    if self.seq_num == self.skip_slot:
                        self.seq_num += 1
                        self.operations.append(None)
                        return

                    client_id = msg_obj['client_id']
                    client_seq = msg_obj['client_seq']
                    # new client or known client with larger client_seq
                    if self.is_valid_new_request(client_id, client_seq):
                        self.clients[client_id] = {}
                        self.clients[client_id]['client_seq'] = client_seq
                        self.clients[client_id]['addr'] = address
                        command = msg_obj['command']
                        command_obj = getCommandObj(client_id, client_seq, address, self.view, self.seq_num, command)
                        self.last_accepted = command_obj
                        self.seq_num += 1
                        command_str = json.dumps(command_obj)
                        self.broadcast_msg(command_str, 'TODO')

                # wait for quorums to accept
                else:
                    pass
            else: # need allegiance from other replicas
                self.followers.clear()
                self.followers.add(self.id)
                del self.prev_proposals[:]
                if self.last_accepted:
                    self.prev_proposals.append(self.last_accepted)
                I_am_leader_obj = getIAMLeaderObj(self.view)
                I_am_leader_str = json.dumps(I_am_leader_obj)
                self.broadcast_msg(I_am_leader_str, 'TODO')

    def is_valid_new_request(self, client_id, client_seq):
        return not client_id in self.clients or client_seq > self.clients[client_id]['client_seq']

    def I_am_the_leader(self):
        return self.view % len(self.replicas) == self.id

    # def handle_msg(self, msg_obj, connection):
    #     message_type = msg_obj['message_type']
    #     if message_type == 'Request':
    #         # only the leader receives Request message from client
    #         # i think i am the leader
    #         if self.view == self.id:
    #             client_id = msg_obj['client_id']
    #             client_seq = msg_obj['client_seq']
    #             if self.is_client_valid(client_id, client_seq):
    #                 self.clients[client_id] = (client_seq) # TODO: client address
    #                 seq = len(self.operations)
    #                 self.votes.append(set())
    #                 self.votes[seq].add(self.id)
    #                 command = msg_obj['command']
    #                 command_msg = getCommandMsg(client_id, client_seq, self.id, seq, command)
    #                 self.last_accepted = json.loads(command_msg)
    #                 self.broadcast_msg(command_msg)
    #     else:
    #         connection.close()
    #         if message_type == 'Command':
    #             leader_id = msg_obj['leader_id']
    #             if leader_id < self.view:
    #                 pass
    #             else:
    #                 client_id = msg_obj['client_id']
    #                 client_seq = msg_obj['client_seq']
    #                 seq_num = msg_obj['seq_num']
    #                 if self.is_client_valid(client_id, client_seq) and self.is_seq_num_valid(seq_num):
    #                     self.votes.append(set())
    #                     self.votes[seq_num].add(leader_id)
    #                     self.votes[seq_num].add(self.id)
    #                     self.clients[client_id] = client_seq
    #                     self.last_accepted = msg_obj
    #                     # broadcast accept message
    #                     accept_msg = getAcceptMsg(self.id, leader_id, client_id, client_seq, seq_num)
    #                     self.broadcast_msg(accept_msg)
    #         elif message_type == 'Accept':
    #             leader_id = msg_obj['leader_id']
    #             if leader_id < self.view:
    #                 pass
    #             else:
    #                 client_id = msg_obj['client_id']
    #                 client_seq = msg_obj['client_seq']
    #                 seq_num = msg_obj['seq_num']
    #                 voter_id = msg_obj['replica_id']
    #                 print('receive accept message ' + str(voter_id))
    #                 if self.clients[client_id] == client_seq and seq_num <= len(self.operations):
    #                     self.votes[seq_num].add(voter_id)
    #
    #                 print('current voters are')
    #                 print('\t' + str(self.votes[seq_num]))
    #                 if len(self.votes[seq_num]) > self.f:
    #                     if len(self.operations) <= seq_num:
    #                         self.operations.append(self.last_accepted['command'])
    #                         # I am the leader
    #                         if self.id == self.view:
    #                             print('send message back to client ????????????? ')
    #                             client_addr = self.clients[client_id][1]
    #                             print(
    #                                 send_message(self.CLIENT_REPLY_MSG.format(self.operations[seq_num], seq_num), None,
    #                                              s=addr))
    #                     self.last_accepted = None
    #         elif message_type == 'IAMLeader':
    #             leader_id = msg_obj['leader_id']
    #             if leader_id > self.view:
    #                 if self.last_accepted:
    #                     client_id = self.last_accepted['client_id']
    #                     client_seq = self.last_accepted['client_seq']
    #                     pre_leader_id = self.last_accepted['leader_id']
    #                     seq_num = self.last_accepted['seq_num']
    #                     command = self.last_accepted['command']
    #                     msg_str = getYouAreLeaderMsg(self.id, client_id, client_seq, pre_leader_id, seq_num. command, command, True)
    #                 else:
    #                     msg_str = getYouAreLeaderMsg(self.id, 0, 0, 0, 0, 0, False)
    #                 self.view = leader_id
    #                 send_message(msg_str, self.replicas[leader_id])
    #         elif message_type == 'YouAreLeader':
    #             follower_id = msg_obj['replica_id']
    #             has_previous = msg_obj['has_previous']
    #             self.followers.add(follower_id)
    #             if has_previous:
    #                 self.prev_proposals.append(msg_obj)
    #
    #             if len(self.followers) > self.f:
    #                 if self.prev_proposals:
    #                     latest_proposal = max(self.prev_proposals, key=lambda item: item['pre_leader_id'])
    #                     latest_proposal['message_type'] = 'Command'
    #                     del latest_proposal['replica_id']
    #                     del latest_proposal['has_previous']
    #                     del latest_proposal['pre_leader_id']
    #                     latest_proposal['leader_id'] = self.id
    #                     self.last_accepted = latest_proposal
    #                     self.clients[self.last_accepted['client_id']] = self.last_accepted['client_seq']
    #                     # TODO: get client address????
    #                     msg_str = json.dumps(latest_proposal)
    #                     self.broadcast_msg(msg_str)
    #
    #
    #
    #
    #
    #         else:
    #             print("WTF" + "=" * 15)
    #             print(msg_obj['message_type'])

    def print_status(self):
        print(self.socket)
        print(self.replicas)

    def is_seq_num_valid(self, seq_num):
        return seq_num >= len(self.operations)


if __name__ == '__main__':
    id = int(sys.argv[1])
    replica = Replica('config1.json', id)
    replica.print_status()
    # replica.connect_other_replicas()
    replica.listen()