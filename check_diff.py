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
        min_len = min(len(msgs[i]), len(msgs[j]))
        if not msgs[i][:min_len] == msgs[j][:min_len]:
            print('Found inconsistent message {} and {}'.format(i, j))