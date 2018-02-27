from helper import *
import sys

class Client:
    HOST = '127.0.0.1'
    DEFAULT_PORT = 7933
    TIMEOUT = 1

    def __init__(self, config_file, id):
        with open(config_file, 'r') as f:
            configs = json.load(f)
            self.replicas = configs['replicas']
        self.replicas = [tuple(replica) for replica in self.replicas]
        self.id = id
        self.seq = 0
        self.view = 0
        self.cur_timeout = Client.TIMEOUT
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(self.cur_timeout)
        self.socket.bind((Client.HOST, Client.DEFAULT_PORT + self.id))


    def request_consensus(self, request_msg):
        leader_id = self.view % len(self.replicas)
        request_obj = getRequestObj(self.id, request_msg, self.seq)
        request_str = json.dumps(request_obj)
        self.socket.sendto(request_str.encode(CODE_METHOD), self.replicas[leader_id])
        finished = False
        while not finished:
            try:
                data, addr = self.socket.recvfrom(BUFFER_SIZE)
                print(data.decode(CODE_METHOD))
                reply_obj = json.loads(data.decode(CODE_METHOD))
                print('{} received a reply seq === {}'.format(self.id, request_obj['client_seq']))
                if reply_obj['message_type'] == 'Reply' and reply_obj['client_id'] == self.id:
                    if reply_obj['client_seq'] == self.seq:
                        print('{} received a finished reply message {}'.format(self.id, reply_obj))
                        finished = True
                    elif reply_obj['client_seq'] > self.seq:
                        print('{} have record before in the system'.format(self.id))
                        self.seq = reply_obj['client_seq'] + 1
                        print('{} send a request with a new seq number {}'.format(self.id, self.seq))
                        request_obj = getRequestObj(self.id, request_msg, self.seq)
                        request_str = json.dumps(request_obj)
                        self.socket.sendto(request_str.encode(CODE_METHOD), self.replicas[leader_id])
            except socket.timeout:
                print('{} get timeout !!!'.format(self.id))
                self.cur_timeout += Client.TIMEOUT
                print('{} increment cur_timeout to {}'.format(self.id, self.cur_timeout))
                self.socket.settimeout(self.cur_timeout)
                self.view += 1
                print('{} rebroadcast request {}'.format(self.id, request_str))
                for replica in self.replicas:
                    self.socket.sendto(request_str.encode(CODE_METHOD), replica)
        self.seq += 1
        self.cur_timeout = TIMEOUT
        self.socket.settimeout(self.cur_timeout)

if __name__ == '__main__':
    mode = sys.argv[1]
    id = int(sys.argv[2])
    client = Client('config1.json', id)
    DEFAULT_MSG = 'cid:{}, cseq:{}'
    if mode == 'm':
        while True:
            user_input = input('Enter your message or \"exit\" to exit: ')
            if user_input == 'exit':
                print('Bye user {}'.format(id))
                break
            else:
                client.request_consensus(user_input)
    elif mode == 's':
        for i in range(500):
            client.request_consensus(DEFAULT_MSG.format(id, i))