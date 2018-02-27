#!/bin/sh

python ReplicaNode.py 0 config1.json > output_0 &
python ReplicaNode.py 1 config1.json > output_1 &
python ReplicaNode.py 2 config1.json > output_2 &