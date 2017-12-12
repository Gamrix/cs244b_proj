from enum import IntEnum
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

        self.append_log_index = 0
        self.append_info = None
        self.debug = False

        self.quorum = (len(pub_keys) - 1) // 3 * 2 + 1  # 2f + 1

    def validate_sig(self, sig_info: SigInfo, data:str):
        public_key = self.pub_keys[sig_info.node_num]
        return (public_key.verify(data, sig_info.sig))

    def sign_message(self, data:str):
        return SigInfo(key.sign(data, ''), self.node_num)

    def check_messages(self):
        """
        Process the next message and send message when needed
        """

        pass

    def pre_append(self, pre_append_message):
        pass
