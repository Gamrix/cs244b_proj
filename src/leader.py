import node
import json

from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto import Random


class Leader(node.Node):

    def __init__(self, *args, **kwargs):
        super(Leader, self).__init__(*args, **kwargs)
        self.message_queue = []
    
    def send_new_message(self, tx: dict):
        self.message_queue.append(tx)
        if self.append_log_index == self.pre_append_log_index:
            # we are now safe to add a new message
            self.pre_append_log_index += 1

            self.hashed_message = 
            
            hash = SHA256.new(text).digest()



