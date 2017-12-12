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

    def send_new_message(self, tx: dict):
        self.message_queue.append(tx)
        if self.append_log_index == self.pre_append_log_index:
            # we are now safe to add a new message
            self.pre_append_log_index += 1

            transactions_str = json.dumps(self.message_queue)
            pre_app_hash = SHA256.new(transactions_str).digest()

            message = [Messages.PRE_APPEND, self.cur_leader_term, self.pre_append_log_index, pre_app_hash]
            self_sign = self.sign_message(json.dumps(message))
            message.append(self_sign)
            self.pre_app_sigs = {self_sign}

            self.pre_append_info = message
            self.append_message = self.message_queue
            self.message_queue = []
            self.broadcast(json.dumps(message))

    def process_pre_app_ack(self, message):
        leader_msg =  json.dumps(self.pre_append_info[:-1])
        if self.validate_sig(message[-1], leader_msg):
            self.pre_app_sigs.add(message[-1])
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
            # need to craft the new commit
            self.commit_messages()
            self.commits.append(commit)
        
        if len(self.pre_app_sigs) >= self.quorum:
            append_proof = list(self.append_sigs)
            self.append_sigs = set()
            append_message = self.append_message
            self.append_message = None

            commit_str = json.dumps(self.commits[-1])            
            commit_hash = SHA256.new(commit_str).digest()
        else:
            append_proof = []
            append_message = ""


