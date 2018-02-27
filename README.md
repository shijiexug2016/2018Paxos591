# Multi-Paxos #

**To start a replica with `<id>` and configuration file `<config>`, and redirect log to a out_`<id>` file**

`python ReplicaNode.py <id> <config> > out_<id>`

**To start a client `<id>` with manual mode**

`python client.py m <id>`

**To start a client `<id>` with script mode**

`python client.py s <id>`

**There is one configuration file `config1.json` ready to use, it tolerant 1 failure replica and skips slot 5**

- To run replicas just run `sh run_replicas.sh`

- To run script mode clients, run `sh run_clients.sh`

