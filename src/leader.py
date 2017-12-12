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

        self.append_message = None

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
       if self.validate_sig(message[-1], json.dumps(message[:-1])):
           self.pre_app_sigs.add(message[-1])
           if len(self.pre_app_sigs) == self.quorum:
               # send append
