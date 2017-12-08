import sys
import socket
from SessionInfo import *
from HAPI_DLL import *
import time
import datetime
from store import *
import MultiServer
from LockLessQueue import LockLessQueue

quit_program = False
es_msg_count = 0
is_msg_count = 0
es_read_count = 1
is_read_count = 1
es_call_count = 0
es_messages = 0
is_messages = 0
# connections to the c;oemts
es_sock = 0
is_sock = 0
# connections to the clients
es_connection = 0
es_index = -1
is_connection = 0
is_index = -1
# esq_from_hydra = Queue.Queue()
# esq_to_hydra = Queue.Queue()
#
# isq_from_hydra = Queue.Queue()
# isq_to_hydra = Queue.Queue()

esms = 0
isms = 0
max_queue_size = 0
started = 0

def set_started():
    global started
    started = True

def es_message_handler(message):
    if started:
        es_messages.add_item(message)
    else:
        if message[8] == 'U': set_started()


def is_message_handler(message):
    is_messages.add_item(message)


def process_es_list():
    while 1:
        if quit_program:
            break
        try:
            esms.incoming_message(es_messages.read_item())
            # print es_messages.read_item()
            # time.sleep(.001)
        except LockLessQueue.EmptyList:

            pass
        except:
            raise


def process_is_list():
    while 1:
        if quit_program:
            break
        try:
            isms.incoming_message(is_messages.read_item())
            # time.sleep(.001)
            # print is_messages.read_item()
        except LockLessQueue.EmptyList:
            pass
        except:
            raise


def es_cleanup_callback():
    ES.OnDisconnect()


def is_cleanup_callback():
    pass


def es_request(message):
    # TODO validate message length before sending
    ES.SendMessage(message)


def is_request(message):
    IS.SendMessage(message)


def interactive_loop():
    while True:
        command = raw_input("")

        if command == 'errors':
            ES.PrintError()

        if command == 'quit' or command == 'q':
            if ES.IsConnected():
                ES.Exit()
            if esms:
                esms.close()
            if isms:
                isms.close()

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
            esms.start()
            isms.start()
            hsi = HySessionInfo(b"algogroup", b"algogroup", b"10.17.240.155", 8701) 
            es_handler = SessionInfo.fnMsgHdlr_t(es_message_handler)
            is_handler = SessionInfo.fnMsgHdlr_t(is_message_handler)
            es_cleanup = SessionInfo.fnCleanUp_t(es_cleanup_callback)
            is_cleanup = SessionInfo.fnCleanUp_t(is_cleanup_callback)

            si = SessionInfo(addressof(hsi), es_handler, is_handler, es_cleanup, is_cleanup)
            hapi = HAPI_DLL(".\\HAPIKIT.DLL", si)
            hapi.Run()

        if command == 'demo' or command == 'd':
            esms.start()
            isms.start()
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

        if command == 'max es lag':
            print es_messages.max_lag

        if command == 'max is lag':
            print is_messages.max_lag

        if command == 'es len':
            print len(es_messages)

        if command == 'is len':
            print len(is_messages)

        if command == 'is lag':
            print is_messages.lag()

        if command == 'es lag':
            print es_messages.lag()


def heartbeat_loop():
    while True:
        if ES.IsConnected():
            # print 'connected, not sending heartbeat'
            ES.SendHeartBeatMessage()
        else:
            ts = time.time()
            st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            # print st + ' ES NOT CONNECTED!!! count = ' + str(es_msg_count)
        if quit_program:
            break
        sleep(5)


if __name__ == "__main__":

    es_messages = LockLessQueue()
    is_messages = LockLessQueue()


    esms = MultiServer.MultiServer('', 10000)
    isms = MultiServer.MultiServer('', 10001)
    esms.set_receiver(es_request)
    isms.set_receiver(is_request)


    print 'starting threads'
    t1 = threading.Thread(target=interactive_loop)
    t1.start()
    print 'interactive started'
    t2 = threading.Thread(target=heartbeat_loop)
    t2.start()
    print 'heartbeat started'
    t3 = threading.Thread(target=process_es_list)
    t3.start()
    t4 = threading.Thread(target=process_is_list)
    t4.start()


    print 'waiting for joins'
    t1.join()
    print 'interactive done'
    t2.join()
    print 'heartbeat done'
    t3.join()
    t4.join()


    print('ALL DONE')

