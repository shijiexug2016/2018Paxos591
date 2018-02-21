import socket
import json
import logging

TIMEOUT = 0.2
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('Paxos')
READY_LOG = 'replica {} is ready at {}'
SEND_LOG = '{} send message: \n\t{} \nto {}'
RECEIVE_LOG = '{} receive message: \n\t{} \nfrom {}'

BUFFER_SIZE = 65536
CODE_METHOD = 'utf-8'

def get_msg_obj(**kwargs):
    return kwargs

def getIAMLeaderObj(view):
    msg_obj = get_msg_obj(message_type='IAmLeader', view=view)
    return msg_obj

def getReplyObj(seq_num, leader_id, client_id):
    msg_obj = get_msg_obj(message_type='Reply', seq_num=seq_num, leader_id=leader_id, client_id=client_id)
    return msg_obj

def getRequestObj(client_id, command, client_seq):
    msg_obj = get_msg_obj(message_type='Request', client_id=client_id, command=command, client_seq=client_seq)
    return msg_obj

def getYouAreLeaderObj(replica_id, client_id, client_seq, pre_view_id, command, seq_num, has_previous):
    msg_obj = get_msg_obj(message_type='YouAreLeader', replica_id=replica_id, has_previous=has_previous, client_id=client_id,
                          client_seq=client_seq, pre_view_id=pre_view_id, seq_num=seq_num, command=command)
    return msg_obj

def getCommandObj(client_id, client_seq, client_addr, view, seq_num, command):
    msg_obj = get_msg_obj(message_type='Command', client_id=client_id, client_seq=client_seq, client_addr=tuple(client_addr), view=view,
                          seq_num=seq_num, command=command)
    return msg_obj


def getAcceptObj(replica_id, view, client_id, client_seq, seq_num):
    msg_obj= get_msg_obj(message_type='Accept', replica_id=replica_id, view=view, client_id=client_id, client_seq=client_seq,
                         seq_num=seq_num)
    return msg_obj

SEND_EXCEPTION_LOG = '{} send to {} exception {}'
RECV_EXCEPTION_LOG = '{} recv exception {}'
SEND_FAIL_LOG = '{} failed to send to {}'
RECV_FAIL_LOG = '{} failed to recv'

def send_msg(sock, message, address, attempts, something_else, retrys = 3):
    if attempts <= retrys:
        try:
            sock.sendto(message.encode(), address)
            return True
        except socket.timeout as e:
            logger.info(SEND_EXCEPTION_LOG.format(sock.getsockname(), address, e))
            attempts += 1
            sock.settimeout(TIMEOUT * attempts)
            send_msg(sock, message, address, attempts, something_else)
    else:
        # TODO: do something with this failure
        logger.info(SEND_FAIL_LOG.format(sock.getsockname(), address))
        return False

def recv_msg(sock, attempts, something_else, retrys = 3):
    msg = ''
    if attempts <= retrys:
        try:
            while True:
                data, address = sock.recvfrom(BUFFER_SIZE)
                msg += data.decode(CODE_METHOD)
                if not len(data) == BUFFER_SIZE:
                   break
            return msg, address
        except socket.timeout as e:
            logger.info(RECV_EXCEPTION_LOG.format(sock.getsockname(), e))
            attempts += 1
            sock.settimeout(TIMEOUT * attempts)
            recv_msg(sock, attempts, something_else)
    else:
        # TODO: do something with this failure
        logger.info(RECV_FAIL_LOG.format(sock.getsockname()))
        return None


def send_message(message, address, s = None):
    if not s:
        print('la'*10)
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            connection.connect(address)
        except Exception as e:
            print(e)
            return False
    else:
        print('na' * 10)
        connection = s

    try:
        print("about to send message !!!!!!!!!!!!!!=======================")
        print('\t' + message)
        connection.send(message.encode(CODE_METHOD))
        return True
    except Exception as e:
        print('????????????????????????')
        print(e)
        return False
    finally:
        connection.close()



def get_message(connection):
    message = ''
    while True:
        data = connection.recv(BUFFER_SIZE)
        message += data.decode(CODE_METHOD)
        if len(data) != BUFFER_SIZE:
            break
    return message
