from enum import IntEnum

class Messages(IntEnum):
    CLIENT_MESSAGE = 1
    PRE_APPEND = 2
    PRE_APPEND_ACK = 3
    APPEND_ENTRIES = 4
    APPEND_ACK = 5
    RETURN_TO_CLIENT = 6



class Node(object):

    def __init__(self, pub_sigs, private_sig, node_num, queues):
        self.node_num = node_num
        self.private_sig = private_sig
        self.pub_sigs = pub_sigs
        self.queues = queues  # Queue is where the messages are simulated to be sent to
     
    def check_messages(self):
        """
        Process the next message and send message when needed
        """

        pass
    
    def pre_append(self, pre_append_message):
        pass
