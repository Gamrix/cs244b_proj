from enum import IntEnum
from collections import namedtuple
import json
import logging
import base64
import time

from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto import Random

class Messages(IntEnum):
    CLIENT_MESSAGE = 1
    PRE_APPEND = 2
    PRE_APPEND_ACK = 3
    APPEND_ENTRIES = 4
    APPEND_ACK = 5
    RETURN_TO_CLIENT = 6

    CATCHUP = 100
    CATCHUP_RESP = 101


SigInfo = namedtuple('SigInfo', ['sig', 'node_num'])

def build_node(cur_time, pub_keys, private_key, node_num, *args, **kwargs):
    logging.basicConfig(filename="logs/{}/node{}.log".format(cur_time, node_num), level=logging.DEBUG)
    node = Node(pub_keys, private_key, node_num, *args, **kwargs)
    node.check_messages()

class Node(object):

    def __init__(self, pub_keys, private_key, node_num, queues, num_nodes):
        self.node_num = node_num
        self.private_key = private_key
        self.pub_keys = pub_keys
        self.queues = queues  # Queue is where the messages are simulated to be sent to
        self.num_nodes = num_nodes
        self.kv_store = {}

        self.cur_leader_term = 0
        self.cur_leader_num = 0
        self.pre_append_log_index = 0
        self.pre_append_info = None
        self.pre_append_hash = None

        # last seen index
        self.pre_append_log_index = 0
        self.append_log_index = 0

        self.append_info = None
        self.debug = False

        self.commits = [""]  # begin with a dummy commit

        self.quorum = (num_nodes - 1) // 3 * 2 + 1  # 2f + 1

    def validate_sig(self, sig_info: SigInfo, data:str):
        sig_info = SigInfo(*sig_info) # turn list back into siginfo
        public_key = self.pub_keys[sig_info.node_num]
        # sig = base64.decodebytes(sig_info.sig.encode("ascii"))
        sig = int(sig_info.sig)
        return (public_key.verify(data.encode("utf-8"), (sig, None)))

    def sign_message(self, data:str):
        sig_bytes = self.private_key.sign(data.encode("utf-8"), '')[0]
        # sig_str = base64.encodebytes(sig_bytes).decode("ascii")
        sig_str = str(sig_bytes) # sig_bytes is int
        return SigInfo(sig_str, self.node_num)

    def send_to_leader(self, message: str):
        logging.debug("Node {} sent to leader: {}".format(self.node_num, message))
        self.queues[self.cur_leader_num].put(message)

    @staticmethod
    def hash_obj(src_obj):
        transactions_str = json.dumps(src_obj)
        hash_bytes = SHA256.new(transactions_str.encode("utf-8")).digest()
        return base64.encodebytes(hash_bytes).decode("ascii")


    def check_messages(self):
        """
        Process the next message and send message when needed
        """
        while True:
            message = json.loads(self.queues[self.node_num].get())
            if message[0] == Messages.PRE_APPEND:
                self.pre_append(message)
            if message[0] == Messages.APPEND_ENTRIES:
                self.append_entries(message)

    # Follower: Handle PreAppendRequest message from leader
    def pre_append(self, pre_append_message):
        #[Messages.PRE_APPEND, self.cur_leader_term, self.pre_append_log_index, pre_app_hash, signature]
        _, term, log_index, hashval, sig = pre_append_message

        # Check sig from leader
        if not self.validate_sig(sig, json.dumps(pre_append_message[:-1])):
            return

        # Update term
        if self.cur_leader_term < term:
            self.cur_leader_term = term

        # Update log index
        if self.pre_append_log_index < log_index:
            self.pre_append_log_index = log_index

        # Log pre_app_hash
        self.pre_append_hash = hashval

        # Record down pre_append_info
        self.pre_append_info = pre_append_message[:-1]

        # Generate, sign, and send ack
        ack_message = [ Messages.PRE_APPEND_ACK ]
        self_sign = self.sign_message(json.dumps(pre_append_message[:-1]))
        ack_message.append(self_sign)
        self.send_to_leader(json.dumps(ack_message))

    # Follower: Handle AppendEntriesRequest message from leader
    def append_entries(self, append_message):
        #[Messages.APPEND_ENTRY, pre_app_proof :[sigs], prev_commit_hash, data, append_proof :[sigs]]
        _, pre_append_proofs, prev_commit_hash, data, append_proofs = append_message

        # Process append_proof
        # Check Append sigs included
        if len(append_proofs) != 0:
            if len(append_proofs) < self.quorum:
                logging.warning("Not enough proofs were sent")
                return

            for proof in append_proofs:
                if (self.append_info == None) or (not self.validate_sig(proof, json.dumps(self.append_info))):
                    logging.warning("Append Proof {} is invalid".format(proof))
                    return

            commit = [self.c_messages, append_proofs, self.append_info]
            self.apply_transactions(commit)

        # Process PreAppend info
        # Check PreAppend sigs included
        if len(pre_append_proofs) == 0:
            # logging.debug("No PreAppends to process")
            return

        for proof in pre_append_proofs:
            if not self.validate_sig(proof, json.dumps(self.pre_append_info)):
                logging.warning("Pre Append Proof {} is invalid".format(proof))
                return

        if self.hash_obj(data) != self.pre_append_hash:
            logging.warning("Transaction hash is invalid")
            return

        if self.append_log_index + 1 == self.pre_append_info[2]:
            self.append_log_index += 1
        else:
            logging.warning("Append log index != PreAppend log index + 1")
            return

        commit_hash = self.hash_obj(self.commits[-1])
        if commit_hash != prev_commit_hash:
            logging.warning("Transactions out of order")
            return

        # Record down append_info
        self.append_info = [Messages.APPEND_ENTRIES, *self.pre_append_info[1:4], prev_commit_hash]

        # Record down data
        self.c_messages = data

        # Generate, sign, and send ack
        ack_message = [ Messages.APPEND_ACK ]
        self_sign = self.sign_message(json.dumps(self.append_info))
        ack_message.append(self_sign)
        self.send_to_leader(json.dumps(ack_message))

    def apply_transactions(self, commit):
        # apply the data
        transactions = commit[0]
        for m, c_num in transactions:
            if "get" in m:
                v = self.kv_store[m["get"]]
                logging.info("Node {} replies {} to client".format(self.node_num, v))

            else:
                v = ""
                self.kv_store.update(m)
            message = [Messages.RETURN_TO_CLIENT, v, self.hash_obj(m)]
            self_sign = self.sign_message(json.dumps(message))
            message.append(self_sign)
            self.queues[c_num].put(json.dumps(message))

        self.commits.append(commit)
