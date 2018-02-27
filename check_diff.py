import json
import os

msgs = []
logs = [f for f in os.listdir('logs') if not f.startswith('.')]
for log in logs:
    with open(os.path.join('logs', log), 'r') as f:
        msg = f.readline().rstrip()
    msgs.append(json.loads(msg))

for i in range(len(msgs)):
    for j in range(i + 1, len(msgs)):
        if not msgs[i] == msgs[j]:
            print('Found inconsistent message {} and {}'.format(i, j))



