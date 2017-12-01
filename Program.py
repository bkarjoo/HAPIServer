import sys
import socket
from SessionInfo import *
from HAPI_DLL import *
import time
import datetime
import Queue
from store import *

quit_program = False
es_msg_count = 0
is_msg_count = 0
es_read_count = 1
is_read_count = 1
es_call_count = 0
# connections to the c;oemts
es_sock = 0
is_sock = 0
# connections to the clients
es_connection = 0
is_connection = 0
es_queue = 0
is_queue = 0


def es_message_handler(message):
    if es_connection:
        # TODO :
        #send message to the client
        es_connection.sendall(message)


def is_message_handler(message):
    if is_connection:
        # send message to the client
        is_connection.sendall(message)


def es_cleanup_callback():
    ES.OnDisconnect()


def is_cleanup_callback():
    pass


def interactive_loop():
    while True:
        command = raw_input("")

        if command == 'errors':
            ES.PrintError()

        if command == 'quit' or command == 'q':
            if ES.IsConnected():
                ES.Exit()
            global quit_program
            quit_program = True
            break

        if command == 'es_status' or command == 'es':

            if ES.IsConnected():

                print("ES is connected.")
            else:
                print("ES is not connected.")

        if command == 'is_status' or command == 'is':

            if IS.IsConnected():
                print("IS is connected.")
            else:
                print("IS is not connected.")

        if command == 'algo' or command == 'a':
            hsi = HySessionInfo(b"algogroup", b"algogroup", b"10.17.240.155", 8701) 
            es_handler = SessionInfo.fnMsgHdlr_t(es_message_handler)
            is_handler = SessionInfo.fnMsgHdlr_t(is_message_handler)
            es_cleanup = SessionInfo.fnCleanUp_t(es_cleanup_callback)
            is_cleanup = SessionInfo.fnCleanUp_t(is_cleanup_callback)

            si = SessionInfo(addressof(hsi), es_handler, is_handler, es_cleanup, is_cleanup)
            hapi = HAPI_DLL(".\\HAPIKIT.DLL", si)
            hapi.Run()

        if command == 'r':
            hsi = HySessionInfo(b"algogroup", b"algogroup", b"10.17.240.155", 8701)
            es_handler = SessionInfo.fnMsgHdlr_t(es_message_handler)
            is_handler = SessionInfo.fnMsgHdlr_t(is_message_handler)
            es_cleanup = SessionInfo.fnCleanUp_t(es_cleanup_callback)
            is_cleanup = SessionInfo.fnCleanUp_t(is_cleanup_callback)

            si = SessionInfo(addressof(hsi), es_handler, is_handler, es_cleanup, is_cleanup)
            hapi = HAPI_DLL(".\\HAPIKIT.DLL", si)
            hapi.dll.PostAdmMsgES(1001, 0, 0)

        if command == 'demo' or command == 'd':
            hsi = HySessionInfo(b"demo", b"demo", b"10.17.240.159", 7620) 

            es_handler = SessionInfo.fnMsgHdlr_t(es_message_handler)
            is_handler = SessionInfo.fnMsgHdlr_t(is_message_handler)
            es_cleanup = SessionInfo.fnCleanUp_t(es_cleanup_callback)
            is_cleanup = SessionInfo.fnCleanUp_t(is_cleanup_callback)

            si = SessionInfo(addressof(hsi), es_handler, is_handler, es_cleanup, is_cleanup)
            hapi = HAPI_DLL(".\\HAPIKIT.DLL", si)
            hapi.Run()

        if command[:6] == 'submit':
            tokens = command.split(' ')
            if len(tokens) != 2:
                return
            # print tokens[1]
            msg = tokens[1].strip().encode()
            ES.SendMessage(msg)


def heartbeat_loop():
    while True:
        if ES.IsConnected():
            # print 'connected, not sending heartbeat'
            ES.SendHeartBeatMessage()
        else:
            ts = time.time()
            st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            print st + ' ES NOT CONNECTED!!! count = ' + str(es_msg_count)
        if quit_program:
            break
        sleep(5)


def accept_new_connection(sock):
    while True:
        if quit_program:
            return None
        try:
            sock.settimeout(0.2)
            sock.listen(1)
            conn, addr = sock.accept()
            return conn
        except socket.timeout:
            continue


def read_request(conn):
    while True:
        if quit_program:
            break
        elif conn:
            try:
                if quit_program:
                    break
                header = conn.recv(13)
                sleep(0.1)
                if header:
                    toks = header.split(':')
                    length = int(toks[3]) - 13
                    remainder = conn.recv(length)
                    whole_message = header + remainder
                    response = 'sending: ' + whole_message
                    conn.sendall(response)
                    return whole_message
            except socket.timeout:
                continue
            except socket.error as err:
                print 'es: ', err
                conn.close()


def write_response(conn, msg):
    try:
        if conn:
            conn.sendall(msg)
    except:
        print 'Error in write_response'
        print conn
        print sys.exc_info()[0]


def listen_for_es_connections():
    global es_sock
    global es_connection

    while (not quit_program):
        if es_connection == 0:
            try:
                es_sock.settimeout(0.2)
                es_sock.listen(1)
                es_connection, client_address = es_sock.accept()
                es_connection.setblocking(1)
                t = threading.Thread(target=listen_for_es_messages())
                t.start()
            except socket.timeout:
                if quit_program:
                    return
        else:
            sleep(1)
            continue


def listen_for_is_connections():
    global is_sock
    global is_connection

    while (not quit_program):
        if is_connection == 0:
            try:
                is_sock.settimeout(0.2)
                is_sock.listen(1)
                is_connection, client_address = is_sock.accept()
                is_connection.setblocking(1)
                t = threading.Thread(target=listen_for_is_messages())
                t.start()
            except socket.timeout:
                if quit_program:
                    return
        else:
            sleep(1)
            continue


def listen_for_es_messages():
    global es_connection
    while True:
        if quit_program:
            break
        elif es_connection:
            try:
                header = es_connection.recv(13)
                if header:
                    length = int(header[10:13]) - 13
                    remainder = es_connection.recv(length)
                    whole_message = header + remainder
                    if whole_message[14:] == 'QUIT':
                        print 'closing es connection.'
                        es_connection.close()
                        es_connection = 0
                        return
                    ES.SendMessage(whole_message)

                if quit_program:
                    break
            except socket.error as err:
                print 'es: ', err
                break
            except:
                print 'error in processing es message'

    es_connection.close()
    es_connection = 0


def listen_for_is_messages():
    global is_connection
    while True:
        if quit_program:
            break
        elif is_connection:
            try:
                header = is_connection.recv(13)
                if header:
                    length = int(header[10:13]) - 13
                    remainder = is_connection.recv(length)
                    whole_message = header + remainder
                    if whole_message[14:] == 'QUIT':
                        print 'closing is Connection.'
                        is_connection.close()
                        is_connection = 0
                        return
                    IS.SendMessage(whole_message)

                if quit_program:
                    break
            except socket.error as err:
                print 'is: ', err
                break
            except:
                print 'error in processing is message'

    is_connection.close()
    is_connection = 0


def create_server_socket(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('localhost', address)
    sock.bind(server_address)
    return sock


def create_es_server_sock():
    global es_sock
    es_sock = create_server_socket(10000)
    while True:
        global es_connection
        es_connection = accept_new_connection(es_sock)
        try:
            while True:
                client_request = read_request(es_sock)
                ES.SendMessage(client_request)
        except socket.error as err:
            print 'is: ', err
        finally:
            is_connection.close()


def create_is_server_sock():
    global is_sock
    is_sock = create_server_socket(10001)
    while True:
        global is_connection
        is_connection = accept_new_connection(is_sock)
        try:
            while True:
                client_request = read_request(is_sock)
                ES.SendMessage(client_request)
        except socket.error as err:
            print 'es: ', err
        finally:
            es_connection.close()


def process_es_queue():
    while True:
        try:
            message = es_queue.get(block=True, timeout=1)
            write_response(es_connection, message)
            insert_message_into_es(message)
            es_queue.task_done()
            if quit_program:
                break
        except Queue.Empty:
            if quit_program:
                break


def process_is_queue():
    while True:
        try:
            message = is_queue.get(block=True, timeout=1)
            write_response(is_connection, message)
            insert_message_into_is(message)
            is_queue.task_done()
            if quit_program:
                break
        except Queue.Empty:
            if quit_program:
                break


if __name__ == "__main__":
    es_queue = Queue.Queue()
    is_queue = Queue.Queue()
    es_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('localhost', 10000)
    es_sock.bind(server_address)
    is_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('localhost', 10001)
    is_sock.bind(server_address)

    print 'starting threads'
    t1 = threading.Thread(target=interactive_loop)
    t1.start()
    print 'interactive started'
    t2 = threading.Thread(target=heartbeat_loop)
    t2.start()
    print 'heartbeat started'
    t3 = threading.Thread(target=listen_for_is_connections)
    t3.start()
    print 'IS socket listener started'
    t4 = threading.Thread(target=listen_for_es_connections)
    t4.start()
    print 'ES socket listener started'
    t5 = threading.Thread(target=process_es_queue)
    t5.start()
    t6 = threading.Thread(target = process_is_queue())
    t6.start()

    print 'waiting for joins'
    t1.join()
    print 'interactive done'
    t2.join()
    print 'heartbeat done'
    t3.join()
    print 'IS done'
    t4.join()
    print 'ES done'
    t5.join()
    t6.join()
    print('ALL DONE')

