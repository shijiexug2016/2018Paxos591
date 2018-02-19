import socket
import json

BUFFER_SIZE = 4096
CODE_METHOD = 'utf-8'

def getIAMLeaderMsg (leader_id):
    msg_obj = {
        'message_type' : 'IAMLeader',
        'leader_id'    : leader_id
    }
    msg_str = json.dumps(msg_obj)
    return msg_str

def getReplyMsg (seq_num, leader_id, client_id):
    msg_obj = {
        'message_type' : 'Reply',
        'sqe_num'      : seq_num,
        'leader_id'    : leader_id,
        'client_id'    : client_id
    }
    msg_str = json.dumps(msg_obj)
    return msg_str

def getRequestMsg (client_id,command,client_seq):
    msg_obj = {
        'message_type' :'Request',
        'client_id'    : client_id,
        'command'      : command,
        'client_seq'   : client_seq
    }
    msg_str = json.dumps(msg_obj)
    return msg_str

def getYouAreLeaderMsg (replica_id, pre_leader_id, command, seq_num):
    msg_obj = {
        'message_type' :'YouAreLeader',
        'replica_id'   : replica_id,
        'pre_leader_id': pre_leader_id,
        'command'      : command,
        'seq_num'      : seq_num
    }
    msg_str = json.dumps(msg_obj)
    return msg_str

def getCommandMsg (client_id, client_seq, leader_id, seq_num, command):
    msg_obj = {
        'message_type' : 'Command',
        'client_id'    : client_id,
        'client_seq'   : client_seq,
        'leader_id'    : leader_id,
        'seq_num'      : seq_num,
        'command'      : command
    }
    msg_str = json.dumps(msg_obj)
    return msg_str

def getAcceptMsg (replica_id,leader_id,client_id,client_seq,seq_num):
    msg_obj = {
        'message_type' : 'Accept',
        'replica_id'   : replica_id,
        'leader_id'    : leader_id,
        'client_id'    : client_id,
        'client_seq'   : client_seq,
        'seq_num'      : seq_num
    }
    msg_str = json.dumps(msg_obj)
    return msg_str

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
