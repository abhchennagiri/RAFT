import SocketServer
import socket
import sys
import Queue as Q
import pickle
from message import Message, RequestVoteData, AppendEntriesData
import time
from datetime import datetime
import threading
import random


#Logger code
import logging

logger  = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter(' %(asctime)s - %(levelname)s - %(message)s ')
#logger.setFormatter( formatter )
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)


IP = "127.0.0.1"                # IP of the localhost

D_PORTS = [
    9000, 
    9001,
    9002                       # Port Numbers for the Datacenters
]

NUM_DC = 3                      # Number of Datacenters.
MAJORITY = 2
MSG_DELAY = 5                   # Delay in number of seconds between for message passing

#States

FOLLOWER = 0
CANDIDATE = 1
LEADER = 2

STATES = [ FOLLOWER, CANDIDATE, LEADER ]

class TCPHandler( SocketServer.BaseRequestHandler ):
         
    def __init__( self, request, client_address, server ):
        logger.debug(" Entering TCP Handler init ")
        SocketServer.BaseRequestHandler.__init__( self, request, client_address, server )
        logger.debug(" Exiting TCP Handler Init ")

  

    def handle(self):
       """
       Each Request is handled by a seperate instantiation of the RequestHandler Object by the handle() method.
       """
       self.data = self.request.recv( 1024 )
       while self.data:  
            msg =  pickle.loads( self.data )
            logger.debug(" msg Details: ")
            logger.debug(" msg.data: " + str( msg.data ) )
            logger.debug(" msg Details over ")
 
            # ClientMessage - Check if this server is the leader. If not, redirect it to the leader

            #### Leader Election Phase ####

            #Election Timeout occurs
            #Server sends a RequestForVote message

            if( msg.message_type == Message.REQUESTVOTE ):
                #Implement the request vote logic
                to_dc = msg.from_dc
                voteGranted = False
                logger.debug(" REQUEST VOTE message received\n ")
                self.server.reset_election_timer()

                if self.server.currentTerm < msg.data.currentTerm:
                    msg.data.term = self.server.currentTerm
                    self.server.currentTerm = msg.data.currentTerm
                    msg.data.voteGranted = True
                    self.server.change_to_follower()

                if( self.server.currentTerm >= msg.data.currentTerm ):
                    if( msg.data.currentTerm not in self.server.votedFor ) or (self.server.votedFor[msg.data.currentTerm] == msg.data.candidateId):
                        if( msg.data.lastLogTerm > self.server.lastLogTerm ) or ( msg.data.lastLogTerm == self.server.lastLogTerm and msg.data.lastLogIndex >= self.server.lastLogIndex ):
                            msg.data.set_results( self.server.currentTerm, True )
                            self.server.send_request_vote_response( to_dc, msg.data )
                            voteGranted = True
                            self.server.votedFor[ msg.data.currentTerm ] = msg.data.candidateId
                            self.server.change_to_follower()

                if voteGranted == False:
                    msg.data.set_results( msg.data.currentTerm, False )
                    self.server.send_request_vote_response( to_dc, msg.data )


                logger.debug(" Resetting the timer messages\n ")
                break

            if( msg.message_type == Message.APPENDENTRIES ):
                #Implement the append entries logic
                break

            if( msg.message_type == Message.REQUESTVOTERESPONSE ):
                #Reset the election timer
                self.server.reset_election_timer()

                if msg.data.term > self.server.currentTerm:
                    self.server.state = FOLLOWER
                    self.server.term = msg.data.term
                    self.server.change_to_follower()
                    logger.debug("Discovered higher term. Stepping down to follower")

                elif msg.data.voteGranted:
                    self.server.votes += 1
                    if self.server.votes > MAJORITY and self.server.state != LEADER:
                        logger.debug(" Server got majority ")
                        self.server.change_to_leader()


                break

            if( msg.message_type == Message.RESPONSE ):
                break    
               
            if( msg.message_type == Message.CLIENTREQ ):
                logger.debug(" Received a ticket request from the client ")
                break
                                    
       self.request.close()    
 
class Datacenter( SocketServer.TCPServer ):
    def __init__(self, server_address, pid, handler_class=TCPHandler):
        #self.logger = logging.getLogger('EchoServer')
        #self.logger.debug('__init__')
        logger.debug("Entering Datacenter Init")
        self.numTickets = 100
        self.q = Q.PriorityQueue()
        self.clock = 0 
        self.numReplies = 0
        self.cli_addr = ( "127.0.0.1", 0 )
        self.state = FOLLOWER
        #TODO: Need to replace everything with the config file
        self.election_timeout = random.randrange( 150, 300, 10 )/20
        self.election_timer = None
        self.votes = 0
        self.reset_election_timer()
        self.pid = pid
        self.currentTerm = 1
        self.votedFor = {}
        self.log = []
        # Index of highest entry known to be committed
        self.commitIndex = 0
        self.lastLogIndex = 0
        self.lastLogTerm = 0

        self.leaderId = 0
        self.heartbeat_timeout = random.randrange( 150, 300, 10 )/20

        #TODO: Define a statemachine, this is going to execute the committed commands in log order.
        self.leader = None                #Identity of last known leader
        SocketServer.TCPServer.__init__( self, server_address, handler_class )
        logger.debug(" Exiting Datacenter Init ")
        return

    def create_socket(self):
        """
        Creates a socket and returns it
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        return s
    
    def getIpPort(self, msg):
        return msg.from_dc[0], msg.from_dc[1]    


    def send_message( self, msg ):
        """ 
        Used by all the the send methods to run as a seperate thread and send it over the network
        """
        logger.debug( "Entering send_message")
        data = pickle.dumps(msg)
        addr = msg.to_dc
        logger.debug( "Sending the msg" + msg + "now" )
        s = self.create_socket()
        s.connect(msg.to_dc)
        s.send(data)
        s.close()
        logger.debug( "Exiting send_message ")
        return True


    """
    Leader Election Methods
    """    

    def start_election(self):
        """
        Start the election once the election_timer becomes zero.
        """
        if self.state == FOLLOWER:
            self.state = CANDIDATE
            self.currentTerm += 1
            self.votes += 1
            data = self.form_request_data()
            self.send_vote_request( data )
            self.reset_election_timer()    

    def reset_election_timer(self):
        """
        Reset the election timer every election_timeout seconds
        """
        if self.election_timer:
                self.election_timer.cancel()
        self.election_timer = threading.Timer( self.election_timeout, self.start_election )
        logger.debug("Election timeout:" + str( self.election_timeout ) )
        self.election_timer.start()        

    def send_vote_request( self, data ):
        """
        If an election timeout happens, the follower will become a candidate and send RequestForVote message to all the other servers.
        """
        logger.debug(" Entering send_vote_request\n ")
        #data_tup = ( 1, self.pid, 10 )
        #Call the form_request_message here 
        logger.debug("Request Data: " + str( data ) )

        from_dc = ( IP, int( D_PORTS[ self.pid - 1 ] ) )
        logger.debug( " from port: " + str( from_dc ) )
        for port in D_PORTS:
            if port != int( from_dc[ 1 ] ):
                msg = Message( Message.REQUESTVOTE, from_dc, ( IP, int( port ) ), 1, data )
                self.send_message( msg )
                
        logger.debug( " Exiting send_vote_request\n " )

    def send_request_vote_response( self, to_dc, data ):
        logger.debug("Entering send_request_vote_response")
        from_dc = ( IP, int( D_PORTS[ self.pid - 1 ] ) )
        msg = Message( Message.REQUESTVOTERESPONSE, from_dc, to_dc, 1, data )
        self.send_message( msg )  
        logger.debug("Exiting send_request_vote_response")  
        pass    

    def form_request_data(self):
        logger.debug("Entering form request message")
        req_data = RequestVoteData( self.pid, self.currentTerm, self.lastLogIndex, self.lastLogTerm )
        return req_data

    def form_append_entries_data( self ):
        logger.debug("Entering form request message")
        append_entry_data = AppendEntriesData( self.pid, self.currentTerm, self.prevLogIndex, self.prevLogTerm, self.commitIndex )    
        return append_entry_data

    def change_to_follower( self ):
        if self.state != FOLLOWER:
            self.state == FOLLOWER
            self.votes = 0
            self.reset_election_timer()

        else:
            logger.debug("Some error in change_to_follower logic")    
        pass

    def change_to_leader( self ):
        if self.state != LEADER:
            #Need to include the leaderID
            self.state = LEADER
            self.leaderId = self.pid
            #TODO: Update the follower details
            self.send_append_entries_to_all()
            self.reset_heartbeat_timer()
            #self
        pass    
    
    def send_append_entries( self, to_dc, data ):
        """
        Send appendEntries to all the peers. Serves as a heartbeat message also.
        """
        from_dc = ( IP, int( D_PORTS[ self.pid - 1 ] ) )
        msg = Message( Message.APPENDENTRIES, from_dc, to_dc, 1, data )
        self.send_message( msg ) 
    
    def send_append_entries_to_all( self ):
        from_dc = ( IP, int( D_PORTS[ self.pid - 1 ] ) )
        data = self.form_append_entries_data( )           
        for port in D_PORTS:
            if port != int( from_dc[ 1 ] ):
                to_dc = ( IP, port )
                self.send_append_entries( to_dc, data )



    def reset_heartbeat_timer( self ):
        """
        This function keeps sending heartbeat after every heartbeat_timeout seconds
        """
        if self.heartbeat_timer:
            self.hearbeat_timer.cancel()
        self.heartbeat_timer = threading.Timer( self.heartbeat_timeout, self.sendAppendEntriesRPC )
        logger.debug("Election timeout:" + str( self.heartbeat_timeout ) )
        self.heartbeat_timer.start()     


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print 'Usage: python process.py <process_num>, process_num = [1,5]'
        sys.exit(0)
    #Setting Logging Parameters
    
    log_file = 'log_dc' + sys.argv[1]
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    """
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    """

    logger.debug("Entering main")
    HOST= "localhost"
    port = D_PORTS[ int(sys.argv[1]) - 1 ]
    logger.debug("Port Number is: " + str(port) )
    pid = int(sys.argv[1])

    while True:
        try:     
            server = Datacenter((HOST, port), pid ,TCPHandler)
            server.serve_forever()
        except KeyboardInterrupt:
            sys.exit(0)    
        
