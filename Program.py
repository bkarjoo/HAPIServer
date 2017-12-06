import sys
import socket
from SessionInfo import *
from HAPI_DLL import *
import time
import datetime
from store import *
import MultiServer

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

def clear_es_messages():
    global es_messages
    global es_index
    es_messages = []
    es_index = -1

def es_message_handler(message):
    if len(es_messages) > 100000:
        if len(es_messages) - es_index == 1:
            clear_es_messages()
    es_messages.append(message)

def clear_is_messages():
    global is_messages
    global is_index
    is_messages = []
    is_index = -1

def is_message_handler(message):
    if len(is_messages) > 10000:
        clear_is_messages()
    is_messages.append(message)


def process_es_list():
    while 1:
        global es_index
        global  max_queue_size
        lag = len(es_messages) - es_index
        if lag > max_queue_size: max_queue_size = lag
        if lag > 1:
            es_index += 1
            esms.incoming_message(es_messages[es_index])
        time.sleep(.001)
        if quit_program:
            break

def process_is_list():
    while 1:
        global is_index
        if len(is_messages) - is_index > 1:
            is_index += 1
            isms.incoming_message(is_messages[is_index])
        time.sleep(.001)
        if quit_program:
            break


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
            if isms:
                isms.close()
            if esms:
                esms.close()
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

        if command == 'es list':
            print es_messages

        if command == 'is list':
            print is_messages

        if command == 'es index':
            print es_index

        if command[:7] == 'es last':
            tokens = command.split(' ')
            try:
                print es_messages[int(tokens[2])]
            except Exception as e:
                print e

        if command == 'is index':
            print is_index

        if command == 'max queue size':
            print max_queue_size

        if command == 'es len':
            print len(es_messages)


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

    es_messages = []
    is_messages = []

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

    esms.quit()
    isms.quit()

    print('ALL DONE')

