import json
import logging
import time
import os


import node
from node import Messages

def build_leader(pub_keys, private_key, node_num, *args, **kwargs):
    cur_time =  time.strftime("%m%d_%H%M")
    os.makedirs("logs/{}/".format(cur_time))

    logging.basicConfig(filename="logs/{}/node{}.log".format(cur_time, node_num), level=logging.DEBUG)
    logging.info("Node is leader")
    node = Leader(pub_keys, private_key, node_num, *args, **kwargs)
    node.check_messages()

class Leader(node.Node):

    def __init__(self, *args,  **kwargs):
        super(Leader, self).__init__(*args, **kwargs)
        self.message_queue = []

        self.commit_info = []
        self.append_message = None
        self.pre_app_sigs = set()
        self.append_sigs = set()

    def broadcast(self, message:str):
        logging.debug("Broadcasted :" + message)
        for i in range(self.num_nodes):
            if i == self.node_num:
                # don't send to self
                continue
            q = self.queues[i]
            q.put(message)

    def send_new_message(self, message): # client_queue:Queue):
        self.message_queue.append(message[1:])
        if self.append_log_index == self.pre_append_log_index:
            # we are now safe to add a new message
            self.pre_append_log_index += 1

            pre_app_hash = self.hash_obj(self.message_queue)

            message = [Messages.PRE_APPEND, self.cur_leader_term, self.pre_append_log_index, pre_app_hash]
            self_sign = self.sign_message(json.dumps(message))
            message.append(self_sign)
            self.pre_app_sigs = {self_sign}

            self.pre_append_info = message[:-1]
            self.append_message = self.message_queue
            self.message_queue = []
            self.broadcast(json.dumps(message))

    def check_messages(self):
        while True:
            message = json.loads(self.queues[self.node_num].get())
            if message[0] == Messages.PRE_APPEND_ACK:
                self.process_pre_app_ack(message)
            if message[0] == Messages.APPEND_ACK:
                self.process_app_ack(message)
            if message[0] == Messages.CLIENT_MESSAGE:
                self.send_new_message(message)


    def process_pre_app_ack(self, message):
        leader_msg =  json.dumps(self.pre_append_info)
        if self.validate_sig(message[-1], leader_msg):
            self.pre_app_sigs.add(node.SigInfo(*message[-1]))
            self.check_send_append()

    def process_app_ack(self, message):
        leader_msg =  json.dumps(self.append_info)
        if self.validate_sig(message[-1], leader_msg):
            self.append_sigs.add(node.SigInfo(*message[-1]))
            self.check_send_append()

    def check_send_append(self):
        send = False
        if len(self.append_sigs) == self.quorum:
            send = True
        elif len(self.pre_app_sigs) >= self.quorum:
            if self.append_log_index == len(self.commits) - 1:
                send = True
        if not send:
            return

        if len(self.append_sigs) == self.quorum:
            # committing needs: transactions, proof of commit,
            # need to craft the new commit
            append_proof = list(self.append_sigs)
            self.append_sigs = set()

            commit = [self.c_messages, append_proof, self.append_info]
            self.apply_transactions(commit)

        else:
            append_proof = []

        if len(self.pre_app_sigs) >= self.quorum:
            pre_app_proof = list(self.pre_app_sigs)
            self.pre_app_sigs = set()
            append_message = self.append_message  # transactions for append phase,
            self.append_message = None

            commit_hash = self.hash_obj(self.commits[-1])
            # now move the preappend data to the append phase
            # commit proof needs items the proof is signing: [APPEND_ENTRY,  term, log_number, d_hash, prev_commit_hash]
            self.append_info = [Messages.APPEND_ENTRIES, *self.pre_append_info[1:4], commit_hash]
            self.append_sigs = {self.sign_message(json.dumps(self.append_info))}
            self.c_messages = append_message
            self.append_log_index += 1
        else:
            pre_app_proof= []
            append_message = ""
            commit_hash = ""

        self.broadcast(json.dumps([Messages.APPEND_ENTRY, pre_app_proof, commit_hash, append_message, append_proof]))
