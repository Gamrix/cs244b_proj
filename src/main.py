from multiprocessing import Process, Queue
import json
import logging
import itertools


from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP  # More secure RSA formulation
from Crypto import Random

import time
import os

import node 
import leader
import prof_tools

def main():
    # logging.basicConfig(level=logging.DEBUG)
    num_nodes = 7
    quorum = (num_nodes -1) // 3 * 2 + 1
    log_time =  time.strftime("%m%d_%H%M")
    os.makedirs("logs/{}/".format(log_time))

    # generate keys 
    random_generator = Random.new().read
    private_keys = [RSA.generate(1024, random_generator) for i in range(num_nodes + 1)]
    public_keys = [k.publickey() for k in private_keys]

    # generate queues
    queues = [Queue() for i in range(num_nodes + 1)]
    client_num = num_nodes
    client_queue = queues[client_num]

    # start up the nodes
    raft_nodes = []

    p = Process(target=leader.build_leader, args=(log_time, public_keys, private_keys[0], 0, queues, num_nodes))
    p.start()
    raft_nodes.append(p)

    for i in range(1, num_nodes):
        p = Process(target=node.build_node, args=(log_time, public_keys,private_keys[i], i, queues, num_nodes))
        p.start()
        raft_nodes.append(p)

    transactions = [{"test": "Hi John"}, {"get": "test"}]
    with prof_tools.time_func("Basic Test 2 tx"):
        run_transactions(transactions, queues[0], quorum , client_num, client_queue)

    print("Benchmark tests")
    with prof_tools.time_func("Test 1 tx"):
        time.sleep(.1)
        while not client_queue.empty(): # reset the queue
            client_queue.get()
        run_transactions(transactions[:1], queues[0], quorum , client_num, client_queue, quiet=True)

    for n in [6, 10, 50, 100, 200, 500, 1000, 2000, 5000, 10000]:
        time.sleep(.5)
        while not client_queue.empty(): # reset the queue
            client_queue.get()

        transactions = [[{"test_{}_{}".format(n, i): "Hi John"},
                         {"get": "test_{}_{}".format(n, i)}] for i in range(n // 2)]

        transactions = list(itertools.chain.from_iterable(transactions))
        with prof_tools.time_func("Test {} tx".format(n)):
            run_transactions(transactions, queues[0], quorum , client_num, client_queue, quiet=True)

def run_transactions(transactions, leader_queue, quorum , client_num, client_queue, quiet=False):
    trans_items = {node.Node.hash_obj(t): [] for t in transactions}

    for t in transactions:
        leader_queue.put(json.dumps([node.Messages.CLIENT_MESSAGE, t, client_num]))

    while True:
        return_val = json.loads(client_queue.get())
        if return_val[2] not in trans_items:
            continue # ignore transactions that don't match (might be from previous run).
        result_log = trans_items[return_val[2]]  # client copy of all results for a tx
        result_log.append(return_val)

        if len(result_log) == quorum:
            if not quiet: print("Finished TX:", result_log)
        if all(len(v) > quorum for v in trans_items.values()):
            if not quiet: print("Transactions finished")
            return


if __name__  == "__main__":
    main()

