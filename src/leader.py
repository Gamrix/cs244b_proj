import json

from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto import Random

import node
from node import Messages


class Leader(node.Node):

    def __init__(self, *args, **kwargs):
        super(Leader, self).__init__(*args, **kwargs)
        self.message_queue = []

        self.commit_info = []
        self.append_message = None
        self.pre_app_sigs = set()
        self.append_sigs = set()

    def broadcast(self, message:str):
        for i, q in enumerate(self.queues):
            if i == self.node_num:
                # don't send to self
                continue
            q.put(message)

    def send_new_message(self, tx: dict, client_queue:Queue):
        self.message_queue.append([tx, client_queue])
        if self.append_log_index == self.pre_append_log_index:
            # we are now safe to add a new message
            self.pre_append_log_index += 1

            transactions_str = json.dumps(self.message_queue)
            pre_app_hash = SHA256.new(transactions_str).digest()

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
            message = json.loads(self.queues[self.node_num].pop())
            if message[0] == Messages.PRE_APPEND_ACK:
                self.process_pre_app_ack(message)
            if message[0] == Messages.APPEND_ACK:
                self.process_app_ack(message)


    def process_pre_app_ack(self, message):
        leader_msg =  json.dumps(self.pre_append_info[:-1])
        if self.validate_sig(message[-1], leader_msg):
            self.pre_app_sigs.add(message[-1])
            self.check_send_append()

    def process_app_ack(self, message):
        leader_msg =  json.dumps(self.append_info)
        if self.validate_sig(message[-1], leader_msg):
            self.app_sigs.add(message[-1])
            self.check_send_append()

    def check_send_append(self):
        send = False
        if len(self.append_sigs) == self.quorum:
            send = True
        elif len(self.pre_app_sigs) >= self.quorum and self.append_log_index == len(self.commits):
            send = True
        if not send:
            return

        if len(self.append_sigs) == self.quorum:
            # committing needs: transactions, proof of commit, 
            # need to craft the new commit
            append_proof = list(self.append_sigs)
            self.append_sigs = set()

            commit = [self.c_messages, append_proof, self.append_info]
            self.apply_transactions(self.c_messages)

        else:
            append_proof = []
        
        if len(self.pre_app_sigs) >= self.quorum:
            pre_app_proof = list(self.pre_app_sigs)
            self.pre_app_sigs = set()
            append_message = self.append_message  # transactions for append phase,
            self.append_message = None

            commit_str = json.dumps(self.commits[-1])            
            commit_hash = SHA256.new(commit_str).digest()
            # now move the preappend data to the append phase
            # commit proof needs items the proof is signing: [APPEND_ENTRY,  term, log_number, d_hash, prev_commit_hash]
            self.append_info = [Messages.APPEND_ENTRY, *self.preappend_info[1:4], prev_commit_hash]
            self.append_sigs = {self.sign_message(json.dumps(self.append_info))}
            self.c_messages = append_message
            self.append_log_index += 1
        else:
            pre_app_proof= []
            append_message = ""
            commit_hash = ""
        
        self.broadcast(json.dumps([Messages.APPEND_ENTRY, pre_app_proof, commit_hash, append_message, append_proof]))

