from multiprocessing import Process, Queue
import json
import logging

from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP  # More secure RSA formulation
from Crypto import Random

import node 
import leader

def main():
    logging.basicConfig(level=logging.DEBUG)
    num_nodes = 7

    # generate keys 
    random_generator = Random.new().read
    private_keys = [RSA.generate(1024, random_generator) for i in range(num_nodes)]
    public_keys = [k.publickey() for k in private_keys]

    # generate queues
    queues = [Queue() for i in range(num_nodes + 1)]
    client_num = num_nodes
    client_queue = queues[client_num]

    # start up the nodes
    p = Process(target=leader.build_leader, args=(public_keys, private_keys[0], 0, queues))
    p.start()

    for i in range(1, num_nodes):
        p = Process(target=node.build_node, args=(public_keys,private_keys[i], i, queues))
        p.start()
    
    queues[0].put(json.dumps([node.Messages.CLIENT_MESSAGE, {"test": "Hi John"}, client_num]))
    queues[0].put(json.dumps([node.Messages.CLIENT_MESSAGE, {"print": "test"}, client_num]))

    while True:
        print(json.loads(client_queue.get()))

if __name__  == "__main__":
    main()
    


