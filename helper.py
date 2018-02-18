import socket
from enum import Enum

BUFFER_SIZE = 4096
class MessageType(Enum):
    IAmLeader = 0
    Decree = 1
    YouAreLeader = 2
    Accept = 3
    Request = 4

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
msg_str = jsonn.dumps(msg_obj)
return msg_str

def getRequest (client_id,command,client_seq):
msg_obj = {
'message_type' :'Request',
'client_id'    : client_id,
'command'      : command,
'client_seq'   : client_seq
}
msg_str = json.dumps(msg_obj)
return msg_str

def getYouAreLeader (replica_id, pre_leader_id, command, seq_num):
msg_obj = {
'message_type' :'YouAreLeader',
'replica_id'   : replica_id,
'pre_leader_id': pre_leader_id,
'command'      : command,
'seq_num'      : seq_num
}
msg_str = json.dumps(msg_obj)
return msg_str

def getCommand (client_id, client_seq, leader_id, seq_num):
msg_obj = {
'message_type' : 'Command',
'client_id'    : client_id,
'client_seq'   : client_seq,
'leader_id'    : leader_id,
'seq_num'      : seq_num
}
msg_str = json.dumps(msg_obj)
return msg_str

def getAccept (replica_id,leader_id,client_id,client_seq,seq_num):
msg_obj = {
'message_type' : 'Accept',
'replica_id'   : replica_id,
'leader_id'    : leader_id,
'client_id'    : client_id,
'client_seq'   : client_seq,
'seq_num'      : sqe_num
}
msg_str = json.dumps(msg_obj)
return msg_str

def send_message(message, address):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(address)
    s.send(message.encode())
    response = receive_message(s)
    s.close()
    return response

def receive_message(connection):
    message = ''
    while True:
        data = connection.recv(BUFFER_SIZE)
        message += data.decode('utf-8')
        if (len(data) != BUFFER_SIZE):
            break
    return message
