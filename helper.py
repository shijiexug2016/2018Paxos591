import socket
import json
import logging
import sys

TIMEOUT = 0.2
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
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

def getReplyObj(client_id, client_seq, seq_num, leader_id, command):
    msg_obj = get_msg_obj(message_type='Reply', client_id=client_id, client_seq=client_seq, seq_num=seq_num, leader_id=leader_id, command=command)
    return msg_obj

def getRequestObj(client_id, command, client_seq):
    msg_obj = get_msg_obj(message_type='Request', client_id=client_id, command=command, client_seq=client_seq)
    return msg_obj

def getYouAreLeaderObj(replica_id, view, prev_accepts):
    msg_obj = get_msg_obj(message_type='YouAreLeader', view=view, replica_id=replica_id, prev_accepts=prev_accepts)
    return msg_obj

def getCommandObj(client_id, client_seq, client_addr, view, seq_num, command, is_noop=False):
    msg_obj = get_msg_obj(message_type='Command', client_id=client_id, client_seq=client_seq, client_addr=tuple(client_addr), view=view,
                          seq_num=seq_num, command=command, is_noop=is_noop)
    return msg_obj

# def getAcceptObj(replica_id, view, client_id, client_seq, seq_num):
#     msg_obj= get_msg_obj(message_type='Accept', replica_id=replica_id, view=view, client_id=client_id, client_seq=client_seq,
#                          seq_num=seq_num)
#     return msg_obj

def getAcceptObj(replica_id, command_obj):
    msg_obj= get_msg_obj(message_type='Accept', replica_id=replica_id, command_obj=command_obj)
    return msg_obj

def getExecutionObj(client_id=-1, client_seq=-1, client_addr=(-1, -1), command='', is_noop=True):
    return get_msg_obj(client_id=client_id, client_seq=client_seq, client_addr=client_addr, command=command, is_noop=is_noop)


SEND_EXCEPTION_LOG = '{} send to {} exception {}'
RECV_EXCEPTION_LOG = '{} recv exception {}'
SEND_FAIL_LOG = '{} failed to send to {}'
RECV_FAIL_LOG = '{} failed to recv'

def send_msg(sock, message, address, attempts, retrys = 3):
    if attempts <= retrys:
        try:
            sock.sendto(message.encode(), address)
            return True
        except socket.timeout as e:
            logger.info(SEND_EXCEPTION_LOG.format(sock.getsockname(), address, e))
            attempts += 1
            sock.settimeout(TIMEOUT * attempts)
            send_msg(sock, message, address, attempts, retrys)
    else:
        logger.info(SEND_FAIL_LOG.format(sock.getsockname(), address))
        return False

def recv_msg(sock, attempts, retrys = 3):
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
            recv_msg(sock, attempts, retrys)
    else:
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
