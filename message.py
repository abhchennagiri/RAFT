"""
Message Class:  Datastructure defined to encapsulate the message.

"""
import time


class Message(object):

    APPENDENTRIES = 0
    REQUESTVOTE = 1
    REQUESTVOTERESPONSE = 2
    RESPONSE = 3
    CLIENTREQ = 4

    MSG_TYPE = [ APPENDENTRIES, REQUESTVOTE, REQUESTVOTERESPONSE, RESPONSE, CLIENTREQ ]


    def __init__(self, message_type, from_dc, to_dc, term,  data=None):
        self.message_type = message_type
        self.from_dc = from_dc
        self.to_dc = to_dc
        self.data = data
        self.term = term
        self.timestamp = int( time.time() )

    def receiver(self):
        return self.to_dc

    def sender(self):
        return self.from_dc

    def data(self):
        return self.data

    def timestamp(self):
        return self.timestamp

    def term(self):
        return self.term

    def type(self):
        return self.message_type

class RequestVoteData(object):
    def __init__( self, pid, currentTerm, lastLogIndex, lastLogTerm):
        self.candidateId = pid
        self.currentTerm = currentTerm
        self.lastLogIndex = lastLogIndex
        self.lastLogTerm = lastLogTerm
        #Results
        self.term = 0
        self.voteGranted = False

    def set_results(self, term, voteGranted ):
        """
        This function implements the results in the RequestVoteRPC
        """
        self.term = term
        self.voteGranted = voteGranted
        pass    


if __name__ == '__main__':
       pass

       
                                  

    
