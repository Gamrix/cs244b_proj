from multiprocessing import Process, Queue
import json

from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto import Random

import node 
import leader

def main():
    num_nodes = 7

    # generate keys 
    random_generator = Random.new().read
    private_keys = [RSA.generate(1024, random_generator) for i in range(num_nodes)]
    public_keys = [k.publickey() for k in private_keys]

    # generate queues
    queues = [Queue() for i in range(num_nodes)]
    client_queue = Queue()

    # start up the nodes
    p = Process(target=leader.build_leader, args=(client_queue, public_keys, private_keys[0], 0, queues))

    for i in range(1, num_nodes):
        p = Process(target=node.build_node, args=(public_keys,private_keys[i], i, queues))
    
    queues[0].put(json.dumps([node.Messages.CLIENT_MESSAGE, {"test": "Hi John"}]))
    queues[0].put(json.dumps([node.Messages.CLIENT_MESSAGE, {"print": "test"}]))

    while True:
        print(json.loads(client_queue.get()))

if __name__  == "__main__":
    main()
    


