from helper import *
from random import *


class Replica:

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
        # correctness of all replicas
        self.correct_replicas = [True] * (len(self.replicas))
        # known clients client id -> client seq
        self.processing_clients = {}
        # next sequence number to propose
        self.next_seq = 0
        # am I elected when system starts
        self.elected = self.view == self.id

        # the current followers
        self.followers = set()
        # is in reelection
        self.is_in_reelection = False
        # (seq, client id, client seq) -> vote count
        self.accepted_commands_count = {}
        # seq -> command with largest view number for this slot
        self.prev_proposals = {}
        # the (client requests, addr) potential leader received while it is not elected
        self.hold_on_requests = []

        # seq -> command
        self.accepted_log = []

        # executions[i] = {
        #     client_id: id
        #     client_seq: seq
        #     is_noop: True/False
        #     command: message
        # }
        self.executions = []
        self.executions_str = []
        # client_id -> {'client_seq' : last client seq, 'seq' : seq corresponding to client_id, client_seq}
        self.executed_clients = {}

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.replicas[self.id])
        logger.info(READY_LOG.format(self.id, self.replicas[self.id]))

    def listen(self):
        while True:
            msg_byte, address = self.socket.recvfrom(BUFFER_SIZE)
            msg_obj = json.loads(msg_byte.decode(CODE_METHOD))

            # simulate message loss
            # random_number = randint(0, 99)
            # logger.info('{} get random number {}'.format(self.id, random_number))
            # if random_number < self.message_loss:
            #     continue

            if address in self.replicas:
                # replica message
                message_type = msg_obj['message_type']
                if message_type == 'Command':
                    self.handle_command(msg_obj)
                elif message_type == 'Accept':
                    self.handle_accept(msg_obj)
                elif message_type == 'IAmLeader':
                    self.handle_new_leader(msg_obj)
                elif message_type == 'YouAreLeader':
                    self.handle_follower(msg_obj)
                else:
                    logger.info(msg_obj)
            else:
                # client message
                self.handle_client(msg_obj, address)

    def get_leader_id(self, view):
        return view % len(self.replicas)

    def is_valid_new_client_request(self, client_id, client_seq):
        # a new client
        if client_id not in self.executed_clients and client_id not in self.processing_clients:
            return True

        # processing, cannot take new request from this client
        if client_id in self.processing_clients:
            logger.debug('{} cannot process seq {}'.format(self.id, client_seq))
            return False

        return client_seq > self.executed_clients[client_id]['client_seq']


    def broadcast_msg(self, msg_str):
        for i in range(len(self.replicas)):
            if not send_msg(self.socket, msg_str, self.replicas[i], 0):
                self.correct_replicas[i] = False

    def handle_client(self, msg_obj, client_address):
        logger.info('{} handle client request {}'.format(self.id, msg_obj))
        client_id = msg_obj['client_id']
        client_seq = msg_obj['client_seq']
        # received an executed client id, client seq, reply immediately
        if client_id in self.executed_clients and client_seq <= self.executed_clients[client_id]['client_seq']:
            logger.info('{} get an executed request cid {}, c_seq {}'.format(self.id, client_id, client_seq))
            logger.info('{} resending the client_id {}, client_seq {}'.format(self.id, client_id, client_seq))
            seq = self.executed_clients[client_id]['seq']
            execution_obj = self.executions[seq]
            client_addr = tuple(execution_obj['client_addr'])
            latest_client_seq = execution_obj['client_seq']
            reply_obj = getReplyObj(client_id, latest_client_seq, seq, self.id, self.executions_str[seq])
            reply_str = json.dumps(reply_obj)
            send_msg(self.socket, reply_str, client_addr, 0)
            return

        logger.info('{} thinks leader is {}'.format(self.id, self.view % len(self.replicas)))
        logger.info('{} elected status is {}'.format(self.id, self.elected))

        if self.id == self.get_leader_id(self.view) and self.elected:
            # skip slot
            if self.next_seq == self.skip_slot:
                logger.info('\tleader {} skipping slot {}'.format(self.id, self.skip_slot))
                self.next_seq += 1

            # not in process request and client seq > last executed client seq
            if self.is_valid_new_client_request(client_id, client_seq):
                logger.info('\tleader {} propose a valid client msg {}'.format(self.id, msg_obj))
                self.processing_clients[client_id] = client_seq
                command = msg_obj['command']
                command_obj = getCommandObj(client_id, client_seq, client_address, self.view, self.next_seq, command)
                command_str = json.dumps(command_obj)
                self.broadcast_msg(command_str)
                self.next_seq += 1
        # received a client message, if I am the leader, start a new election
        else:
            # not in reelection
            if not self.is_in_reelection:
                new_view = self.view + 1
                if self.id == self.get_leader_id(new_view):
                    logger.info('{} start a new election'.format(self.id))
                    # clear previous info and set is_in_reelection to True
                    self.is_in_reelection = True
                    self.elected = False
                    self.followers = set()
                    self.accepted_commands_count = {}
                    self.prev_proposals = {}
                    # clear the clients who was in processing
                    self.processing_clients = {}
                    i_am_leader_obj = getIAMLeaderObj(new_view)
                    i_am_leader_str = json.dumps(i_am_leader_obj)
                    self.view = new_view
                    self.broadcast_msg(i_am_leader_str)

            if self.id == self.get_leader_id(self.view):
                self.hold_on_requests.append((msg_obj, client_address))

    def handle_new_leader(self, msg_obj):
        logger.info('{} handle \"IAmLeader\" message {}'.format(self.id, msg_obj))
        view = msg_obj['view']

        # ignore smaller view message
        if self.view > view:
            logger.info('\t{} received a view number {} <= current view {}'.format(self.id, view, self.view))
            return

        new_leader_id = self.get_leader_id(view)

        # not new leader, clear all leader info records
        if not self.is_in_reelection and not self.id == new_leader_id:
            self.elected = False
            self.is_in_reelection = True
            self.followers = set()
            self.accepted_commands_count = {}
            self.prev_proposals = {}
            self.hold_on_requests = []
            # clear the clients who was in processing
            self.processing_clients = {}
            logger.info('\t{} received a new leader message from {}'.format(self.id, new_leader_id))
            self.view = view

        last_accepted_log = self.accepted_log[len(self.executions): len(self.accepted_log)]
        you_are_leader_obj = getYouAreLeaderObj(self.id, self.view, last_accepted_log)
        you_are_leader_str = json.dumps(you_are_leader_obj)
        logger.info('\t{} send you are leader message with recent accepted log {} to {}'.format(self.id, last_accepted_log, new_leader_id))

        if not send_msg(self.socket, you_are_leader_str, self.replicas[new_leader_id], 0):
            # leader failed
            pass

    def handle_follower(self, msg_obj):
        logger.info('{} handle follower allegiance message {}'.format(self.id, msg_obj))
        view = msg_obj['view']
        if self.view > view:
            logger.info('\t{} received a invalid follower message'.format(self.id))
            return

        if self.view < view:
            # I am no longer the leader
            logger.info('{} is no longer the leader'.format(self.id))
            self.elected = False
            self.is_in_reelection = False
            self.followers = set()
            self.accepted_commands_count = {}
            self.prev_proposals = {}
            self.hold_on_requests = []
            self.view = view
            return

        # received you are leader message correctly
        replica_id = msg_obj['replica_id']
        self.followers.add(replica_id)
        follower_accepted_log = msg_obj['prev_accepts']
        # get all latest follower accepted log entries
        for temp_command in follower_accepted_log:
            temp_seq_num = temp_command['seq_num']
            temp_view = temp_command['view']
            if temp_seq_num not in self.prev_proposals:
                self.prev_proposals[temp_seq_num] = temp_command
            else:
                recorded_view = self.prev_proposals[temp_seq_num]['view']
                if temp_view > recorded_view:
                    self.prev_proposals[temp_seq_num] = temp_command

        if not self.elected and len(self.followers) > self.f:
            logger.info('{} is elected as leader !!!!!'.format(self.id))
            self.is_in_reelection = False
            self.elected = True
            if len(self.prev_proposals) > 0:
                # propose previous commands
                for seq, command in self.prev_proposals.items():
                    command['view'] = self.view
                    old_command_str = json.dumps(command)
                    self.broadcast_msg(old_command_str)
            else:
                # propose new commands
                for client_request, client_address in self.hold_on_requests:
                    client_id = client_request['client_id']
                    client_seq = client_request['client_seq']
                    command = client_request['command']
                    command_obj = getCommandObj(client_id, client_seq, client_address, self.view, self.next_seq, command)
                    new_command_str = json.dumps(command_obj)
                    self.broadcast_msg(new_command_str)
            del self.hold_on_requests[:]

    def handle_command(self, msg_obj):
        logger.info('{} handle \"Command\" message {}'.format(self.id, msg_obj))
        view = msg_obj['view']

        if self.view > view:
            logger.info('\t{} skip smaller view current = {}, given = {}'.format(self.id, self.view, view))
            return

        if self.view < view:
            # TODO: do something

            logger.info('\t{} detect a leader change'.format(self.id))

        if self.is_in_reelection and not self.id == self.get_leader_id(view):
            self.is_in_reelection = False

        seq_num = msg_obj['seq_num']
        client_id = msg_obj['client_id']
        client_seq = msg_obj['client_seq']
        if seq_num >= len(self.accepted_log):
            # update processing client info
            self.processing_clients[client_id] = client_seq
            # add holes to log if there is any
            while len(self.accepted_log) < seq_num:
                logger.info('\t{} find hole {} in accepted log, append a noop'.format(self.id, len(self.accepted_log)))
                noop_command_obj = getCommandObj(-1, -1, [], -1, len(self.accepted_log), '')
                self.accepted_log.append(noop_command_obj)
            logger.info('\t{} append new command'.format(self.id))
            self.accepted_log.append(msg_obj)
        else:
            logger.info('\t{} update a command to a new view {} at {}'.format(self.id, view, seq_num))
            self.accepted_log[seq_num] = msg_obj
        # update next seq number
        self.next_seq = max(self.next_seq, seq_num)

        accept_obj = getAcceptObj(self.id, msg_obj)
        accept_str = json.dumps(accept_obj)
        logger.info('\t{} broadcast the accept message {}'.format(self.id, accept_obj))
        self.broadcast_msg(accept_str)

    def handle_accept(self, msg_obj):
        logger.info('{} handle \"Accept\" message {}'.format(self.id, msg_obj))
        view = msg_obj['command_obj']['view']

        if self.view > view:
            return

        if self.view < view:
            logger.info('\t{} is no longer the leader'.format(self.id))
            # I am no longer the leader
            self.elected = False
            self.is_in_reelection = False
            self.followers = set()
            self.accepted_commands_count = {}
            self.prev_proposals = {}
            self.hold_on_requests = []
            self.view = view
            self.accepted_commands_count = {}

        seq_num = msg_obj['command_obj']['seq_num']
        client_id = msg_obj['command_obj']['client_id']
        client_seq = msg_obj['command_obj']['client_seq']
        client_addr = tuple(msg_obj['command_obj']['client_addr'])

        # the execution of the seq_num is already learned
        if seq_num < len(self.executions) and not self.executions[seq_num]['is_noop']:
            logger.info('\t{} received an \"Accept\" message {} which is executed'.format(self.id, msg_obj))
            return

        # not learned yet
        command_id = (seq_num, client_id, client_seq)
        if command_id not in self.accepted_commands_count:
            self.accepted_commands_count[command_id] = 1
        else:
            self.accepted_commands_count[command_id] += 1
        logger.info('\t{} update {} count to {}'.format(self.id, command_id, self.accepted_commands_count[command_id]))

        # a majority of acceptors have accepted command
        if self.accepted_commands_count[command_id] > self.f:
            logger.info('{} find command {} ready to execute at {}'.format(self.id, msg_obj['command_obj'], seq_num))
            # detect holes in executions
            if seq_num > len(self.executions):
                logger.info('{} find hole(s), my execution len = {}, given seq num = {}'.format(self.id, len(self.executions), seq_num))
                while len(self.executions) < seq_num:
                    noop_obj = getExecutionObj()
                    self.executions.append(noop_obj)
                    self.executions_str.append('NOOP')

            # after filled holes
            command = msg_obj['command_obj']['command']
            execution_obj = getExecutionObj(client_id, client_seq, client_addr, command, False)
            if seq_num < len(self.executions) and self.executions[seq_num]['is_noop']:
                logger.info('{} fill a execution is noop previously at {} with {}'.format(self.id, seq_num, msg_obj['command_obj']))
                self.executions[seq_num] = execution_obj
                self.executions_str[seq_num] = execution_obj['command']
            else:
                logger.info('{} append new execution {} at {}'.format(self.id, msg_obj['command_obj'], seq_num))
                self.executions.append(execution_obj)
                self.executions_str.append(execution_obj['command'])

            logger.info('\t{} take client {} off processing, add to executed_clients'.format(self.id, client_id))
            #self.update_clients(client_id, client_seq, self.executed_clients)
            self.executed_clients[client_id] = {}
            self.executed_clients[client_id]['client_seq'] = client_seq
            self.executed_clients[client_id]['seq'] = seq_num
            logger.debug('\t\t\tPROCESSING_CLIENTS {}'.format(self.processing_clients))
            if client_id in self.processing_clients:
                del self.processing_clients[client_id]

            logger.info('{} write executions to file'.format(self.id))
            with open('logs/log_{}'.format(self.id), 'w') as f:
                f.write(json.dumps(self.executions_str) + '\n')
                f.write('executed_clients {}'.format(json.dumps(self.executed_clients)) + '\n')
                f.write('processing_clients {}'.format(json.dumps(self.processing_clients)) + '\n')

            self.next_seq = max(self.next_seq, len(self.executions))
            # client_addr = msg_obj['command_obj']['client_addr']
            if self.id == self.get_leader_id(self.view) and self.elected:
                reply_obj = getReplyObj(client_id, client_seq, seq_num, self.id, execution_obj['command'])
                reply_str = json.dumps(reply_obj)
                logger.debug('leader {} sending REPLY MESSAGE {}'.format(self.id, reply_obj))
                send_msg(self.socket, reply_str, client_addr, 0)

if __name__ == '__main__':
    id = int(sys.argv[1])
    replica = Replica('config1.json', id)
    replica.listen()