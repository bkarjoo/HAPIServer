import socket
import thread
import threading
import time
import Queue


class MultiServer(object):
    def __init__(self, host, port):
        print 'CREATING', id(self)
        self.host = host
        self.port = port
        self.sock = socket.socket()
        self.connections = []
        self.incoming_queue = Queue.Queue()
        self.outgoing_queue = Queue.Queue()
        self.quit_time = False
        self.receiver = 0
        self.listener_thread = 0
        self.incoming_thread = 0
        self.outgoing_thread = 0
        # self.console_thread = 0

    def incoming_message(self, message):
        self.incoming_queue.put(message)

    def set_receiver(self, receiver):
        self.receiver = receiver

    def process_incoming(self):
        # process the incoming queue pass messages to clients with .001 delay
        while 1:
            if self.quit_time:
                print 'Quiting process_incoming', id(self)
            try:
                message = self.incoming_queue.get(timeout=1)
                for c in self.connections:
                    try:
                        c.send(message)
                        time.sleep(.001)
                    except:
                        pass #if client issue don't do nothing with that client
                self.incoming_queue.task_done()
                if self.quit_time:
                    print 'Quiting process_incoming', id(self)
                    break
            except Queue.Empty:
                time.sleep(.002)
                if self.quit_time:
                    print 'Quiting process_incoming', id(self)
                    break

    def process_outgoing(self):
        # process ourgoing queues pass messages to hydra with .005 delay
        while 1:
            try:

                message = self.outgoing_queue.get(block=True, timeout=1)
                if self.receiver:

                    while len(message) > 0:
                        print message
                        msg_len = int(message[10:13])
                        self.receiver(message[:msg_len])
                        message = message[msg_len:]

                    time.sleep(.001)
                self.outgoing_queue.task_done()
                if self.quit_time:
                    break
            except Queue.Empty:
                if self.quit_time:
                    break

    def client_listener(self, conn):
        # each client gets one of these
        while True:
            try:
                #print self.quit_time
                if self.quit_time:
                    print 'Quiting client_listener', id(self)
                conn.settimeout(1)
                # TODO take 13 and then the rest, ensures only the message comes in
                request = conn.recv(1024)  # 1024 stands for bytes of data to be received
                if len(request) == 0:
                    conn.close()
                    break
                if request.upper() == 'QUIT':
                    conn.close()
                    break
                self.outgoing_queue.put(request)
            except socket.timeout:
                if self.quit_time:
                    print 'Quiting client_listener', id(self)
                pass
            except:
                break

    def listen_for_clients(self):
        self.sock.settimeout(1)
        while 1:
            try:
                #print 'listening for clients'

                if self.quit_time: break
                conn, addr = self.sock.accept()
                conn.setblocking(1)
                self.connections.append(conn)
                # Creating new thread. Calling clientthread function for this function and passing conn as argument.
                thread.start_new_thread(self.client_listener,(
                    conn, ))  # start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
            except socket.timeout:
                if self.quit_time:
                    print 'Quiting listen_for_clients', id(self)
                pass
            except:
                raise

    def user_prompt(self):
        while 1:
            user_input = raw_input('>>')
            if user_input.upper() == 'Q':
                self.quit_time = True
                break
            for c in self.connections:
                self.incoming_queue.put(user_input)
        self.close()
        print 'Multiserver is now closed.'


    def start(self):
        self.sock.bind((self.host, self.port))
        self.sock.listen(5)
        self.listener_thread = threading.Thread(target=self.listen_for_clients)
        self.incoming_thread = threading.Thread(target=self.process_incoming)
        self.outgoing_thread = threading.Thread(target=self.process_outgoing)
        #self.console_thread = Thread(target=self.user_prompt)
        self.incoming_thread.start()
        self.outgoing_thread.start()
        self.listener_thread.start()
        #self.console_thread.start()

    def close(self):
        self.quit_time = True
        print self.quit_time
        print 'ID in MAIN', id(self)

        print 'closing connections:', self
        time.sleep(1)
        for c in self.connections:
            if c:
                c.close()

        print 'waiting for listen_for_clients thread'
        if self.listener_thread: self.listener_thread.join()
        print 'waiting for process_outgoing_thread '
        if self.outgoing_thread: self.outgoing_thread.join()
        print 'waiting for process_incoming_thread'
        if self.incoming_thread: self.incoming_thread.join()

        print 'closing socket'

        time.sleep(1)
        if self.sock:
            self.sock.close()