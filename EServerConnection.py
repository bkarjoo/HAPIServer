import time
import datetime
import msgpack
import zmq

from UserInfo import *
from ctypes import *

errors = list()


ADMMSG_RECONNECT = 1001
ADMMSG_TRACE_TOGGLE = 1002
DEF_DISP_QTY = 100

class EServerConnection:

    def __init__(self):
        self.nLastParent = 0
        self.uiES = UserInfo()
        self.sDefAcct = ''
        self.nParentIDs = []
        self.nLastParent = 0
        self.print_on = True

        context = zmq.Context()
        pub = context.socket(zmq.PUB)
        port = pub.bind_to_random_port('tcp://*')

        self.pub = pub
        self.url = 'tcp://127.0.0.1:' + str(port)

        self.U_flag = False
        self.previously_connected = False


        # EXPERIMENTAL
        self.session_start_timestamp = datetime.datetime.now()
        self.cur_buying_power = 1000 # B, T orders reduce BP by same amt. BC, S orders do NOTHING to BP

    def set_print_on(self, on_off_bool):
        self.print_on = on_off_bool

    def setHAPI(self, HAPI):
        self.HAPI = HAPI

    def getNextID(self):
        nLastParent = self.nLastParent
        nParentIDs = self.nParentIDs
        nLastParent += 1
        nRet = nLastParent
        nParentIDs.append( nLastParent )
        return nLastParent

    def popParent(self, nPID ):
        try:
            if self.nLastParent < nPID:
                self.nLastParent = nPID

            self.nParentIDs.remove(nPID)
        except:
            #print("ParentID {0} was not used".format( nPID ))
            pass

            # getting this a lot on startup for some reason
            # why?

    def HandleESMsg(self,lMsg):
        msg = lMsg.split(':')

        if msg[2] == 'Z':
            errors.append(lMsg)

        if msg[2] == 'S' and msg[4] == 'S' and msg[14] == 'L':
            pass
        elif msg[2] == 'F' and msg[4] in ['E', 'X']:
            pass
        elif msg[2] in ['U','D','L','A','P']:
            pass
        elif not self.U_flag: # TURN OFF message REPLAY!:
            return


        # END of Login
        if msg[2] == 'U': #End of Login
            if self.print_on:
                print('ES: -------------------- ES Login Processing done -----------------\n')

            self.U_flag = True # replay done

    

    def PrintError(self):
        for m in errors:
            print m
        print 'no more errors.'

    def GetUserInfo(self):
        return self.uiES

    def GetAvailChan(self):
        UpLinks = 'UP LINKS:   '
        DnLinks = 'DOWN LINKS: '

        for i,l in self.uiES.Links.items():
            if l.Stat == 'U':
                UpLinks += i + ' '
            elif l.Stat == 'D':
                DnLinks += i + ' '
        print( UpLinks )
        print( DnLinks )

    def IsConnected(self):
        try:
            c_uint_p = POINTER(c_uint)
            rv = self.HAPI.dll.GetESConnState()
            if rv == 0: return 0
            return cast(rv, c_uint_p).contents.value
        except:
            return False
    
    def CheckForETBSymb(self,sSymb):
        return sSymb in self.uiES.ETB

    def CheckMaxValues(self, Acct, Symb, sQty, sPr):
        MxS = self.uiES.MaxShares.get( Acct + '~' + Symb )
        
        if  MxS != None:
            if  (int(MxS.MaxShr) > 0 and int(MxS.MaxShr) < int(sQty)) or \
                (float(MxS.MaxAmt) > 0 and float(MxS.MaxAmt) < (float(sQty)*float(sPr))):
                return False
        
        return True

    def GetLocQtyInSymb(self, Symb):
        Loc = self.uiES.Locates.get( Symb )
        if Loc != None:
            return Loc.nQtyAvail
        else:
            return None


 #    Traceback (most recent call last):
  # File "C:\Python27x86\lib\threading.py", line 801, in __bootstrap_inner
    # self.run()
  # File "C:\Python27x86\lib\threading.py", line 754, in run
    # self.__target(*self.__args, **self.__kwargs)
  # File "HAPIPY.py", line 385, in passive_thread
    # ES.SendCancelMsg(acct, passive_ticker, passive_buy_order_id, 'B') # later in the loop, rejoin the new best bid
  # File "E:\git\trading\zt\HAPIPY\EServerConnection.py", line 405, in SendCancelMsg
    # parent = orig_order.PNo
# AttributeError: 'NoneType' object has no attribute 'PNo'

    def SendCancelMsg(self, acct, ticker, order_id, side):

        # what does this first number mean, and the 069
        # #:02924:N:069:ABCD:0:1000058:C:MSFT:B:100:43.250::OUCH:DAY::L:ANY:Y:*

        if order_id == '': # simplify
            return
        
        #orig_order = self.uiES.Ords[acct][ticker][order_id]

        if side in ['B', 'BC']:
            key = 'buy'
        else:
            key = 'sell'
        orig_order = self.uiES.Ords[acct][ticker][key]
        if orig_order == None:
            return # concurrency workaround. not perfect


        parent = orig_order.PNo
        ticker = orig_order.Symb
        side = orig_order.Side
        size = orig_order.Qty
        px = orig_order.Pr
        chan = orig_order.Chan
        tif = orig_order.TIF
        o_type = orig_order.OType
        disp = orig_order.Disp

            
        # #:02924:N:069:ABCD:0:1000058:C:MSFT:B:100:43.250::OUCH:DAY::L:ANY:Y:*
        #:00000:N:000:DEMOX1:1:500022:C:NOK:B:100:1.0::DAY:L:::ANY:Y:*
        
        cancel_msg = "#:00000:N:000:{0}:{1}:{2}:C:{3}:{4}:{5}:{6}::{7}:{8}::{9}:ANY:{10}:*".format( \
                    acct, parent, order_id, ticker, side,
                    size, px, chan,
                    tif, o_type, disp).encode() # send original size, per david


        RVal = self.HAPI.dll.SendMsgES(cancel_msg)

        if self.print_on:
            print('SENDING CANCEL: order {0} ({1} of {2} {3} @ {4})'.format(order_id, side, orig_order.nLvsQty, ticker, px))
        #print ("ES: Cancel={0} - {1}".format(RVal, cancel_msg))

    def SendETBReqMsg(self, acct):
        etb_msg = ':00000:F:000:E:0:0:{0}:*'.format(acct).encode()
        RVal = self.HAPI.dll.SendMsgES(etb_msg)
        if self.print_on:
            print('SENDING ETB REQUEST')

    # ES: Send=83 - #:00000:N:000:DEMOX1:5::N:GDX:B:2866:22.59::CSFB:DAY::L::Y::100:::9,,,,,:8:GDX::::*

    # check cur buyign power before sending msg

    def SendESOrderMsg(self, sAcct='', sSymb='', sSd='', sQty='', sPr='', sContra='', sChan='', sTIF='', sOType='', sDisp ='', sCanRepID=''):
        if self.IsConnected() == 0:
            if self.previously_connected == True:
                if self.print_on:
                    print("ES: Connection is DOWN. trying to reconnect...")
                self.U_flag = False # assuming i get a U on reconnect
                self.ReconnectSvr()
            else:
                if self.print_on:
                    print('ES: Connection NOT YET UP')
            return
        else:
            self.previously_connected = True

        if self.CheckMaxValues( sAcct, sSymb, sQty, sPr ) == False:
            if self.print_on:
                print("ES: Price and Quantity exceeds Max values set for this account for this symbol")
            return
        # Side if not BUY, determine if it will be long Sell or Short


        if sSd not in ['B', 'BC']:
            mpPos = self.uiES.OvPos.get(sAcct)
            if mpPos is None:
                sSd = 'T'
            else:
                Pos = mpPos.get(sSymb)
                if Pos is not None and Pos.Qty > int( sQty ):
                    sSd = 'S'
                else:
                    sSd = 'T'

        # if Short and HTB, chk for Available Locate Qty
        if sSd == 'T' and self.CheckForETBSymb( sSymb ) == False and sCanRepID == '': # only decrement locates if NOT a can/rep

            # the above check seems wrong
            # need to verify
            
            if self.uiES.Locates.get(sAcct) == None:
                Loc = None
            else:
                Loc = self.uiES.Locates[sAcct].get( sSymb ) # whole time, just wasn't indexing into acct first...



              # File "E:\git\trading\zt\HAPIPY\EServerConnection.py", line 453, in SendESOrderMsg
                # Loc = self.uiES.Locates[sAcct].get( sSymb ) # whole time, just wasn't indexing into acct first...
             #    KeyError: 'DEMOX1'

            if Loc != None:
                nLocQty = Loc.nQtyAvail
                if  nLocQty == None or (nLocQty != None and nLocQty < int(sQty)):
                    if self.print_on:
                        print("ES: Not Enough Located Qty {1} for SHORTING HTB Symb={0}".format( sSymb, nLocQty ) )
                    return
                else:
                    # deplete this qty from avail qty before sending out
                    self.uiES.Locates[sAcct][sSymb].UsedQty(int(sQty))

            else:
                if self.print_on:
                    print("ES: Not Enough Located Qty 0 for SHORTING HTB Symb={0}".format( sSymb ) )
                return

            
        sParent = ''
        sParent = str(self.getNextID())


        #:14418:N:000:ABCDEF:0:0:N:JOSB:T:500:59.12::GSAC:DAY::M::Y::300::0:3,15-59-52,16-00-00:8:JOSB::Q:USD:*
        #:00000:N:000:DEMOX1:1::N:NOK:B:100:1.0::CSFB:DAY::L::Y::100::::8:NOK::::*

        # NOTE: documentation is mislabeled for disp qty, as Max Floor Amt. use field 17 for display quantity
        # NOTE: for csfb PATHFINDER: use 9,,,,,
            # for csfb CROSSFINDER: use 4A,,,,,
        sOrderMsg = "#:00000:N:000:{0}:{1}::N:{2}:{3}:{4}:{5}:{6}:{7}:{8}::{9}::{10}::{12}:{11}::4A,,,,,:8:{2}::::*".format( \
                    sAcct, sParent, sSymb, sSd,
                    sQty, sPr, sContra, sChan,
                    sTIF, sOType, sDisp, sCanRepID, DEF_DISP_QTY).encode() # CanRepID required in cancel/replace orders. blank otherwise
        

        print(sOrderMsg)
        RVal    = self.HAPI.dll.SendMsgES(sOrderMsg)

        if self.print_on:
            print 'SENDING ORDER: {0} {1} {2} @ {3}'.format(sSd, sQty, sSymb, sPr)
        #print ("ES: Send={0} - {1}".format(RVal, sOrderMsg))

    def PrintPositionsList(self,sAcct='',sSymb=''):
        if self.print_on:
            print ('-------- Positions ----------')
        if sAcct == 'ALL' or sAcct == '':
            for k,sl in self.uiES.OvPos.items():
                for kk,l in sl.items():
                    if self.print_on:
                        print("ES: POS {0} {1} {2} {3} {4} {5}".format( l.Acct, l.Symb, l.Qty, l.Pr, l.LocInf.nQtyTot, l.LocInf.nQtyAvail ) )
            

        else:
            SymbList = self.uiES.OvPos.get( sAcct )

            if SymbList != None:
                if( sSymb ):
                    Pos = SymbList.get( sSymb )
                    if Pos != None:
                        if self.print_on:
                            print("ES: POS {0} {1} {2} {3} {4} {5}".format( sAcct, sSymb, Pos.Qty, Pos.Pr, Pos.LocInf.nQtyTot, Pos.LocInf.nQtyAvail ) )
                else:
                    for k,v in SymbList.items():
                        if self.print_on:
                            print ("ES: POS {0} {1} {2} {3} {4} {5}".format( v.Acct, v.Symb, v.Qty, v.Pr, v.LocInf.nQtyTot, v.LocInf.nQtyAvail ) )
            else:
                if self.print_on:
                    print('ES: Found no Positions with Account, {0}\n'.format( sAcct ) )


    def OnDisconnect(self):
        #self.uiES.OvPos.clear() # why would i clear these?
        #self.uiES.Locates.clear()
        self.session_start_timestamp = datetime.datetime.now()
        self.U_flag = False
        if self.print_on:
            print("ES: DISCONNECTED from ES.")

    def SetTrace(self, bTurnOn=True ):
        if bTurnOn == True:
            self.HAPI.dll.PostAdmMsgES(ADMMSG_TRACE_TOGGLE, 0, 7 )
        else:
            self.HAPI.dll.PostAdmMsgES(ADMMSG_TRACE_TOGGLE, 0, 0 )
    
    def ReconnectSvr(self):
        self.HAPI.dll.PostAdmMsgES(ADMMSG_RECONNECT, 0, 0 )

    def Exit(self):
        m = '#:25329:O:015:*'.encode()
        self.HAPI.dll.SendMsgES(m)
        time.sleep(1)
        self.HAPI.dll.HYDRAPIKITExit()

        #self.HAPI.dll.Exit()
        #self.HAPI.dll.HAPIKITExit()

        # print('sending logout message')
        # logout_message = '#:00000:O:015:*'
        #
        # self.HAPI.dll.SendMsgES(logout_message)
        # sleep(1)
        # ret = int()
        # print('calling EndSession')
        # self.HAPI.dll.EndSession(ret)
        # sleep(1)
        # return ret


    def SendHeartBeatMessage(self):
        """
        no need to send this, hapikit sends this, the returned T message shows connection
        :return:
        """
        try:
            self.HAPI.dll.SendMsgES('#:00000:H:000:*')
        except:
            pass

    def SendMessage(self, msg):
        try:
            self.HAPI.dll.SendMsgES(msg.encode())
        except:
            print('Message submission failed.')

