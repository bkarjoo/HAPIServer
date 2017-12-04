import sys
import socket
from SessionInfo import *
from HAPI_DLL import *
import time
import datetime
import Queue
from store import *
import MultiServer

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
# esq_from_hydra = Queue.Queue()
# esq_to_hydra = Queue.Queue()
#
# isq_from_hydra = Queue.Queue()
# isq_to_hydra = Queue.Queue()

esms = 0
isms = 0


def es_message_handler(message):
    esms.incoming_message(message)


def is_message_handler(message):
    isms.incoming_message(message)


def es_cleanup_callback():
    ES.OnDisconnect()


def is_cleanup_callback():
    pass


def es_request(message):
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
            isms.quit()
            esms.quit()
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


if __name__ == "__main__":


    es_queue = Queue.Queue()
    is_queue = Queue.Queue()

    esms = MultiServer.MultiServer('', 10000)
    isms = MultiServer.MultiServer('', 10001)
    esms.set_receiver(es_request)
    isms.set_receiver(is_request)
    esms.start()
    isms.start()

    print 'starting threads'
    t1 = threading.Thread(target=interactive_loop)
    t1.start()
    print 'interactive started'
    t2 = threading.Thread(target=heartbeat_loop)
    t2.start()
    print 'heartbeat started'



    print 'waiting for joins'
    t1.join()
    print 'interactive done'
    t2.join()
    print 'heartbeat done'

    print('ALL DONE')

