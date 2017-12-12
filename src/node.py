from enum import IntEnum
from collections import namedtuple
import json

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

def build_node(*args, **kwargs):
    node = Node(*args, **kwargs)
    node.check_messages()

class Node(object):

    def __init__(self, pub_keys, private_key, node_num, queues):
        self.node_num = node_num
        self.private_key = private_key
        self.pub_keys = pub_keys
        self.queues = queues  # Queue is where the messages are simulated to be sent to
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

        self.quorum = (len(pub_keys) - 1) // 3 * 2 + 1  # 2f + 1

    def validate_sig(self, sig_info: SigInfo, data:str):
        public_key = self.pub_keys[sig_info.node_num]
        return (public_key.verify(data, sig_info.sig))

    def sign_message(self, data:str):
        return SigInfo(key.sign(data, ''), self.node_num)

    def send_to_leader(self, message: str):
        self.queues[self.cur_leader_num].put(message)

    def check_messages(self):
        """
        Process the next message and send message when needed
        """
        while True:
            message = json.loads(self.queues[self.node_num].pop())
            if message[0] == Messages.PRE_APPEND:
                self.pre_append(message)
            if message[0] == Messages.APPEND_ENTRIES:
                self.append_entries(message)

    # Follower: Handle PreAppendRequest message from leader
    def pre_append(self, pre_append_message):
        #[Messages.PRE_APPEND, self.cur_leader_term, self.pre_append_log_index, pre_app_hash, signature]
        _, term, log_index, hashval, sig = pre_append_message

        # Check sig from leader
        if not validate_sig(sig, json.dumps(pre_append_message[:-1])):
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
        #[Messages.APPEND_ENTRIES, pre_append_proof (list of sigs), data, prev_commit_hash, append_proof (list of sigs)]
        _, pre_append_proofs, data, prev_commit_hash, append_proofs = append_message

        # Process append_proof
        # Check Append sigs included
        for proof in append_proofs:
            if (self.append_info == None) or (not validate_sig(proof, json.dumps(self.append_info))):
                return

        # Process PreAppend info
        # Check PreAppend sigs included
        for proof in pre_append_proofs:
            if not validate_sig(proof, json.dumps(self.pre_append_info)):
                return

        # Check data hash sent in by PreAppend
        if SHA256.new(json.dumps(data)).digest() != self.pre_append_hash:
            return

        # Check previous commit hash
        if self.prev_commit_hash != prev_commit_hash:
            return

        # Record down append_info
        self.append_info = [Messages.APPEND_ENTRIES, *self.preappend_info[1:4], prev_commit_hash]

        # Generate, sign, and send ack
        ack_message = [ Messages.APPEND_ACK ]
        self_sign = self.sign_message(json.dumps(self.append_info))
        ack_message.append(self_sign)
        self.send_to_leader(json.dumps(ack_message))

    def apply_transactions(self, transactions):
        # apply the data
        for m, c_queue in transactions:
            if "print" in m:
                v = self.kv_store[m["print"]]
                print("Node {} replies {} to client".format(self.node_num, v))

            else:
                v = ""
                self.kv_store.update(m)
            message = [Messages.RETURN_TO_CLIENT, v]
            self_sign = self.sign_message(json.dumps(message))
            message.append(self_sign)
            c_queue.push(json.dumps(message))

        self.commits.append(commit)
