from socket import *
# Importing all from thread
from thread import *
from threading import *
import time
import Queue

# Defining server address and port
host = ''  # 'localhost' or '127.0.0.1' or '' are all same
port = 52000  # Use port > 1024, below it all are reserved
quit_program = False
# Creating socket object
sock = socket()
# Binding socket to a address. bind() takes tuple of host and port.
sock.bind((host, port))
# Listening at the address
sock.listen(5)  # 5 denotes the number of clients can queue

conns = []

request_queue = Queue.Queue()

def clientthread():
    # infinite loop so that function do not terminate and thread do not end.
    while True:
        # Sending message to connected client
        for c in conns:
            c.send('Hi! I am server\n')  # send only takes string
        # Receiving from client
        #data = conn.recv(1024)  # 1024 stands for bytes of data to be received
        #print data
        time.sleep(5)

def clientlisten(conn):
    while True:
        request = conn.recv(1024)  # 1024 stands for bytes of data to be received
        request_queue.put(request)

def process_request_queue():
    while True:
        try:
            message = request_queue.get(block=True, timeout=1)
            print message
            request_queue.task_done()
            if quit_program:
                break
        except Queue.Empty:
            if quit_program:
                break

t = Thread(target = clientthread)
t.start()
t2 = Thread(target = process_request_queue)
t2.start()

while True:
    # Accepting incoming connections
    conn, addr = sock.accept()
    # Creating new thread. Calling clientthread function for this function and passing conn as argument.
    start_new_thread(clientlisten, (conn,))  # start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
    conns.append(conn)

for c in conns:
    c.close()

sock.close()

t.join()
t2.join()

print 'DONE!'