import Pyro4
import netifaces as ni
from datetime import datetime
from collections import defaultdict
import socket
from common import *
import threading
import os.path as op
import time
import struct
from threading import Thread
import pickle


@Pyro4.expose
class VersionController(object):

    def __init__(self, host, port, k, broadcast):
        self.host = host
        self.port = port
        self.k = k
        self.files = {}
        self.id = None
        self.serversTable = {}
        self.partitionTable = {} # dict[id]={0:[ids de servidores en esa particion],..}
        self.starting = True
        self.coord = None
        self.heartbeats = 0
        self.lastReplicateServer = -1 # last server for k-replication
        self.versionTable = {} # dict[id]={'file1':[1,2,3,4],..}
        self.serviceBroadcast = broadcast

    def getStartingValue(self):
        return self.starting

    def setRunning(self):
        self.starting = False
    
    def getServerID(self):
        return self.id

    def getHOST(self):
        return self.host

    def getPORT(self):
        return self.port

    def setPORT(self, value):
        self.port = value

    def getCOORD(self):
        return self.coord

    # Services
    def commit(self, file, name, id):
        self.addFile(file, name, id)
        print(self.files)


    def checkout(self, name, id, time):
        version = {}
        if self.coord['id']==self.id:
            # Search for last commit in all servers
            serversIds = self.getServersVersion(name, id, time)
            # Set message and notify broadcaster
            self.serviceBroadcast.setMessage(Checkout(client=id, name=name, timestamp=time, ids=serversIds))
            self.canSend()
            # Receive and return
            timeout = time.time() + 30   # 30 sec from now
            received = False
            while not received:
                if(self.serviceBroadcast.theresMessage()):
                    for i in range(len(self.serviceBroadcast.messageQueue)):
                        if self.serviceBroadcast.messageQueue[i]!=0:
                            continue
                        if self.serviceBroadcast.messageQueue[i].name==name and self.serviceBroadcast.messageQueue[i].client==id and self.serviceBroadcast.messageQueue[i].timestamp==time:
                            msg = self.serviceBroadcast.messageQueue.pop(i)
                            recent_version['file'] = msg.file
                            recent_version['date'] = msg.timestamp
                            received = True
                            break

                if time.time() > timeout:
                    recent_version['error']= 'timeout: no respose for update'
                    break
            
        else:    
            key = name + ':' + id
            if(key in self.files):
                for version in self.files[key]:
                    # print(version)
                    date_time_obj = datetime.strptime(time, '%m/%d/%Y %H:%M:%S')
                    stamp = datetime.timestamp(date_time_obj)
                    # print(stamp)
                    if(int(version['timestamp']) == int(stamp)):
                        print(version)
                        return version
        return version

    def update(self, name, id):
        recent_version = {}
        if self.coord['id']==self.id:
            # Search for last commit in all servers
            serversIds = self.getServersVersion(name, id)
            # Set message and notify broadcaster
            self.serviceBroadcast.setMessage(Update(client=id, name=name, ids=serversIds))
            self.canSend()
            # Receive and return
            timeout = time.time() + 30   # 30 sec from now
            received = False
            while not received:
                if(self.serviceBroadcast.theresMessage()):
                    for i in range(len(self.serviceBroadcast.messageQueue)):
                        if self.serviceBroadcast.messageQueue[i]!=0:
                            continue
                        if self.serviceBroadcast.messageQueue[i].name==name and self.serviceBroadcast.messageQueue[i].client==id:
                            msg = self.serviceBroadcast.messageQueue.pop(i)
                            recent_version['file'] = msg.file
                            recent_version['date'] = msg.timestamp
                            received = True
                            break

                if time.time() > timeout:
                    recent_version['error']= 'timeout: no respose for update'
                    break

        else:
            recent_version = self.getRecentVersion(name, id)
        return recent_version

    def getVersions(self, name, id):
        # Returns a dictionary: {datetime, file}
        versions = {}
        key = name + ':' + id
        if(key in self.files):
            versions[name] = []
            for version in self.files[key]:
                versionObj = {}
                date = datetime.fromtimestamp(version['timestamp'])
                date_time = date.strftime('%m/%d/%Y %H:%M:%S')
                versionObj['datetime'] = date_time
                versionObj['file'] = version['file']
                versions[name].append(versionObj)
        print(versions)
        return versions

    def addFile(self, file, name, id):
        now = datetime.now()
        # date_time = now.strftime('%m/%d/%Y %H:%M:%S')
        timestamp = datetime.timestamp(now)
        fileInfo = {'file': file, 'timestamp': timestamp}
        index = name + ':' + id
        if index in self.files:
            self.files[index].append(fileInfo)
        else:
            self.files[index] = [fileInfo]

    def getRecentVersion(self, name, id):
        key = name + ':' + id
        recent_timestamp = 0
        recent_version = {}
        recent_to_return = {}
        if(key in self.files):
            for version in self.files[key]:
                if(int(version['timestamp']) >= recent_timestamp):
                    recent_version = version
                    recent_timestamp = version['timestamp']

        if recent_version:
            date = datetime.fromtimestamp(recent_version['timestamp'])
            date_time = date.strftime('%m/%d/%Y %H:%M:%S')
            recent_to_return['file'] = recent_version['file']
            recent_to_return['date'] = date_time
        print(recent_to_return)
        return recent_to_return

    def getID(self):
        config = open(op.join(op.dirname(op.abspath(__file__)), "config.txt"),"r")
        HOST = config.readline().strip('\n')    # The remote host
        PORT = int(config.readline().strip('\n'))          # The same port as used by the server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            data = s.recv(4096)
            self.id = data.decode()
        print('Received', repr(data))

    def sendCommit(self, file, name, id):
        while not self.coord:
            pass
        
        if self.coord['id']==self.id:
            nextReplicateServer = getNextReplicateServer(self.lastReplicateServer, self.serversTable)
            self.lastReplicateServer = nextReplicateServer
        else:
            nextReplicateServer = getNextReplicateServer(self.id, self.serversTable)

        ipPortaux = self.serversTable[nextReplicateServer]
        ipPort = ipPortaux.split(':')
        HOST = ipPort[0]
        PORT = ipPort[1]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            data = {
                'file': file,
                'name': name,
                'id': id
            }
            encoded_data = pickle.dumps(data)
            s.sendall(encoded_data)
            data = s.recv(1024)

            # s.bind((HOST, int(PORT)))
            # s.listen(1)
            # while True:
            #     conn, addr = s.accept()
            #     with conn:
            #         print('Connected by', addr)
            #         conn.sendall(str(self.getID()).encode())


    def getServersVersion(self, name, id, version=None):
        # version => checkout, else update
        # Buscar la última version del archivo
        recentVersion = version
        if not version: # Update
            recentVersion = 0
            for server in self.versionTable: # dict[id]={'user:file1':[1,2,3,4],..}
                key = str(id) + name
                if key in self.versionTable[server]:
                    for timestamp in self.versionTable[server][key]:
                        if int(timestamp) >= recentVersion:
                            recentVersion = int(timestamp)

        # Buscar los que tienen la version reciente
        servers = []
        for server in self.versionTable:
            key = str(id) + name
            if key in self.versionTable[server]:
                for timestamp in self.versionTable[server][key]:
                    if int(timestamp) == recentVersion:
                        servers.append(server)
        return servers

class broadcast(Thread):
    def __init__(self, server):
        Thread.__init__(self)
        self.daemon = True
        self.response = None
        self.message = None
        self.server = server
        self.messageType = -1
        self.endTransmission = False
        self.messageQueue = []
        self.send = False
        self.start()

    def canSend(self):
        self.send = True
    
    def cantSend(self):
        self.send = False

    def setMessage(self, msg):
        self.messageType = msg.code
        self.message = pickle.dumps(msg)
    
    def getResponse(self):
        return self.response

    def clearResponse(self):
        self.response = None
    
    def getQueuedMessage(self):
        if(len(self.messageQueue) > 0):
            return self.messageQueue.pop(0)
        return None

    def theresMessage(self):
        if(len(self.messageQueue) > 0):
            return True
        return False

    def getEndTransmission(self):
        endTransmissionInfo = {
            'endTransmission': self.endTransmission,
            'messageType': self.messageType
        }
        return endTransmissionInfo

    def setEndTransmission(self,value):
        self.endTransmission = value

    def run(self):
        # message = b'very important data'
        multicast_group = ('224.10.10.10', 10000)

        # Create the datagram socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Set a timeout so the socket does not block
        # indefinitely when trying to receive data.
        sock.settimeout(2)
        # Set the time-to-live for messages to 1 so they do not
        # go past the local network segment.
        ttl = struct.pack('b', 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

        while True:
            #TODO: Debo setear que no pueda enviar despues recibir los ack -> depende del numero del mensaje recibo n acks
            # ya esta seteado el timeout asi que despues de eso debo contar los ack
            # en caso de enviar el id, no cuento los ack
            if(self.send):
                try:

                    # Send data to the multicast group
                    print('sending {!r}'.format(self.message))
                    sent = sock.sendto(self.message, multicast_group)

                    # Look for responses from all recipients
                    while True:
                        print('waiting to receive')
                        try:
                            data, server = sock.recvfrom(4096)
                            data = pickle.loads(data)
                            if(data.code == 0):
                                self.response = IdMessage(data.id,data.ip,data.port)
                                self.messageQueue.append(self.response)
                                self.clearResponse()
                            elif(data.code == 2):
                                self.response = CoordMessage(data.id,data.ip,data.port)
                                self.messageQueue.append(self.response)
                                self.clearResponse()
                            elif(data.code == 3):
                                self.response = ACKMessage(data.responseTo)
                                print('ack to ' + str(data.responseTo))
                                self.messageQueue.append(self.response)
                                self.clearResponse()
                                # Con esto puedo contar los mensajes en el hilo principal
                                # para saber que no soy coord => debo tener > 1
                            elif(data.code == 4):
                                self.response = Heartbeat(id=data.id)
                                print('heartbeat de!!!!!!!!!!!!!: ' + str(data.id))
                                self.messageQueue.append(self.response)
                                self.clearResponse()
                        except socket.timeout: 
                            #TODO: Si messageType son 0 => ended Id transmission
                            # si coord == None => comienzo eleccion
                            # Al inicio endedIdTransmission is false, then here is turned to true
                            # y en el if del hilo principal en false
                            # Si messageType es de eleccion, se debe recibir el numero de acks correspondientes
                            # a los servidores con id menor a mi
                            # Para estar bien debo recibir mas de un ack(contando el mio), sino soy coord
                            print('timed out, no more responses')
                            self.setEndTransmission(True)
                            self.cantSend()
                            break
                        else:
                            print('received {!r} from {}'.format(
                                data, server))

                finally:
                    pass
                    # print('closing socket')
                    # sock.close()

class receive(Thread):

    def __init__(self,server):
        Thread.__init__(self)
        self.daemon = True
        self.HOSTID = -1
        self.serverInfoMsg = None
        self.server = server
        self.received = False
        self.receivedMsg = None
        self.messageType = -1
        self.messageQueue = []
        self.heartbeatReceived = False
        self.electionMsgReceived = False
        self.start()

    def getMessage(self):
        return self.receivedMsg

    def setServerInfo(self, id, ip, port):
        self.serverInfoMsg = IdMessage(id, ip, port)
        self.HOSTID = int(id)

    def clearMessage(self):
        self.receivedMsg = None

    def getQueuedMessage(self):
        if(len(self.messageQueue) > 0):
            return self.messageQueue.pop(0)
        return None

    def theresMessage(self):
        if(len(self.messageQueue) > 0):
            return True
        return False

    def run(self):
        multicast_group = '224.10.10.10'
        connected = False

        # Create the socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Reusa el address para que sea todos contra todos
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_address = ('', 10000)
        sock.bind(server_address)
        group = socket.inet_aton(multicast_group)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        sock.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            mreq)

        # Receive/respond loop
        while True:
            print('\nwaiting to receive message\n')
            data, address = sock.recvfrom(4096)

            print('received {} bytes from {}'.format(
                len(data), address))
            data = pickle.loads(data)
            print(data)
            if(data.code == 0):
                self.receivedMsg = IdMessage(data.id,data.ip,data.port)
                print('sending server info to', address)
                self.messageQueue.append(self.receivedMsg)
                self.clearMessage()
                if(self.server.coord and self.server.coord['id'] == self.server.getServerID()):
                    sock.sendto(pickle.dumps(CoordMessage(self.server.getServerID(),self.server.getHOST(),self.server.getPORT())), address)
                # Si en el receive recibo un codigo 0 => respondo con la info del server
                sock.sendto(pickle.dumps(self.serverInfoMsg), address)
            elif(data.code == 1):
                if(int(data.id) > self.HOSTID):
                    print('sending acknowledgement for election to', address)
                    sock.sendto(pickle.dumps(ACKMessage(1)), address)
                    if(not self.electionMsgReceived):
                        self.receivedMsg = ElectionMessage(data.id)
                        self.messageQueue.append(self.receivedMsg)
            elif(data.code == 2):
                self.electionMsgReceived = False
                self.receivedMsg = CoordMessage(data.id,data.ip,data.port)
                self.messageQueue.append(self.receivedMsg)
            elif(data.code == 4):
                self.heartbeatReceived = True
                self.receivedMessage = Heartbeat(versionTable=data.versionTable, serversTable=data.serversTable)
                print('SENDING ANSWER TO HBB!!!! ' + str(self.server.getServerID()))
                self.messageQueue.append(self.receivedMessage)
                sock.sendto(pickle.dumps(Heartbeat(id=self.server.getServerID())), address)
            else:
                # Si recibo un mensaje de eleccion data.code == 3 => envio ack y ya
                # si mi id es mayor lo ignoro ==> NO ENVIO ACK
                print('sending acknowledgement to', address)
                sock.sendto(pickle.dumps(ACKMessage(-1)), address)
        
class broadcastService(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.response = None
        self.message = None
        self.messageType = -1
        self.endTransmission = False
        self.messageQueue = []
        self.send = False
        self.start()

    def canSend(self):
        self.send = True
    
    def cantSend(self):
        self.send = False

    def setMessage(self, msg):
        self.messageType = msg.code
        self.message = pickle.dumps(msg)
    
    def getResponse(self):
        return self.response

    def clearResponse(self):
        self.response = None
    
    def getQueuedMessage(self):
        if(len(self.messageQueue) > 0):
            return self.messageQueue.pop(0)
        return None

    def theresMessage(self):
        if(len(self.messageQueue) > 0):
            return True
        return False

    def getEndTransmission(self):
        endTransmissionInfo = {
            'endTransmission': self.endTransmission,
            'messageType': self.messageType
        }
        return endTransmissionInfo

    def setEndTransmission(self,value):
        self.endTransmission = value

    def run(self):
        # message = b'very important data'
        multicast_group = ('224.11.11.11', 10000)

        # Create the datagram socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Set a timeout so the socket does not block
        # indefinitely when trying to receive data.
        sock.settimeout(2)
        # Set the time-to-live for messages to 1 so they do not
        # go past the local network segment.
        ttl = struct.pack('b', 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

        while True:
            #TODO: Debo setear que no pueda enviar despues recibir los ack -> depende del numero del mensaje recibo n acks
            # ya esta seteado el timeout asi que despues de eso debo contar los ack
            # en caso de enviar el id, no cuento los ack
            if(self.send):
                try:

                    # Send data to the multicast group
                    print('sending service {!r}'.format(self.message))
                    sent = sock.sendto(self.message, multicast_group)

                    # Look for responses from all recipients
                    while True:
                        print('waiting to receive service')
                        try:
                            data, server = sock.recvfrom(4096)
                            data = pickle.loads(data)
                            if(data.code == 5):
                                self.response = Update(client=data.client, name=data.name, file=data.file, timestamp=data.timestamp)
                                self.messageQueue.append(self.response)
                                self.clearResponse()
                                break # First one is enough
                            elif(data.code == 6):
                                self.response = Checkout(client=data.client, name=data.name, file=data.file, timestamp=data.timestamp)
                                self.messageQueue.append(self.response)
                                self.clearResponse()
                                break
                            elif(data.code == 7): # ESTO PUEDE SERVIR PARA COMMIT CON BROADCAST
                                self.response = ACKMessage(data.responseTo)
                                print('ack to ' + str(data.responseTo))
                                self.messageQueue.append(self.response)
                                self.clearResponse()
                                # Con esto puedo contar los mensajes en el hilo principal
                                # para saber que no soy coord => debo tener > 1
                        except socket.timeout: 
                            #TODO: Si messageType son 0 => ended Id transmission
                            # si coord == None => comienzo eleccion
                            # Al inicio endedIdTransmission is false, then here is turned to true
                            # y en el if del hilo principal en false
                            # Si messageType es de eleccion, se debe recibir el numero de acks correspondientes
                            # a los servidores con id menor a mi
                            # Para estar bien debo recibir mas de un ack(contando el mio), sino soy coord
                            print('timed out, no more services responses')
                            self.setEndTransmission(True)
                            self.cantSend()
                            break
                        else:
                            print('received service {!r} from {}'.format(
                                data, server))

                finally:
                    pass
                    # print('closing socket')
                    # sock.close()

class receiveService(Thread):

    def __init__(self, server):
        Thread.__init__(self)
        self.daemon = True
        self.HOSTID = -1
        self.server = server
        self.serverInfoMsg = None
        self.received = False
        self.receivedMsg = None
        self.messageType = -1
        self.messageQueue = []
        self.heartbeatReceived = False
        self.electionMsgReceived = False
        self.start()

    def getMessage(self):
        return self.receivedMsg

    def setServerInfo(self, id, ip, port):
        self.serverInfoMsg = IdMessage(id, ip, port)
        self.HOSTID = int(id)

    def clearMessage(self):
        self.receivedMsg = None

    def getQueuedMessage(self):
        if(len(self.messageQueue) > 0):
            return self.messageQueue.pop(0)
        return None

    def theresMessage(self):
        if(len(self.messageQueue) > 0):
            return True
        return False

    def run(self):
        multicast_group = '224.11.11.11'
        connected = False

        # Create the socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Reusa el address para que sea todos contra todos
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_address = ('', 10000)
        sock.bind(server_address)
        group = socket.inet_aton(multicast_group)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        sock.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            mreq)

        # Receive/respond loop
        while True:
            print('\nwaiting to receive service message\n')
            data, address = sock.recvfrom(4096)

            print('received service {} bytes from {}'.format(
                len(data), address))
            data = pickle.loads(data)
            print(data)
            if data.code>4:
                if self.server.id in data.ids:
                    if(data.code == 5): # Update
                        # Find file
                        file = self.server.update(data.name, data.client)
                        self.receivedMsg = Update(client=data.client, name=data.name, file=file['file'], timestamp=file['date'])
                        print('sending update of '+ data.name +' to ', address)
                        sock.sendto(pickle.dumps(self.receivedMsg), address)
                    elif(data.code == 6): # Checkout
                        file = self.server.checkout(data.name, data.client, data.timestamp)
                        print('sending checkout of '+ data.name +' at '+ data.time +' to ', address)
                        sock.sendto(pickle.dumps(Checkout(client=data.client, name=data.name, file=file['file'], timestamp=data.timestamp)), address)
                    elif(data.code == 7): # Commit
                        self.electionMsgReceived = False
                        self.receivedMsg = CoordMessage(data.id,data.ip,data.port)
                        self.messageQueue.append(self.receivedMsg)
                    else:
                        # Si recibo un mensaje de eleccion data.code == 3 => envio ack y ya
                        # si mi id es mayor lo ignoro ==> NO ENVIO ACK
                        print('sending acknowledgement to', address)
                        sock.sendto(pickle.dumps(ACKMessage(-1)), address)
            

class IdMessage:
    def __init__(self, id, ip, port):
        self.code = 0
        self.id = id
        self.ip = ip
        self.port = port

    def __repr__(self):
        return 'Server ID: ' + self.id + ' ' + 'IP: ' + self.ip

class ElectionMessage:
    def __init__(self, id):
        self.code = 1
        self.id = id

    def __repr__(self):
        return 'Election Server ID: ' + self.id

class CoordMessage:
    def __init__(self, id, ip, port):
        self.code = 2
        self.id = id
        self.ip = ip
        self.port = port
    def __repr__(self):
        return 'COORD Server ID: ' + self.id + ' ' + 'IP: ' + self.ip


class ACKMessage:
    def __init__(self,code):
        self.code = 3
        self.responseTo = code
    def __repr__(self):
        return 'ack'

class Heartbeat:
    def __init__(self, serversTable=None, versionTable=None, id=None):
        self.code = 4
        self.id = id
        self.versionTable = versionTable
        self.serversTable = serversTable
    def __repr__(self):
        return 'Heartbeat:::: ' + str(self.code) + ' ' + str(self.id)

class Update:
    def __init__(self, client, name, file=None, timestamp=None, ids=None):
        self.code = 5
        self.client = client
        self.name = name
        self.file = file
        self.timestamp = timestamp
        self.ids = ids
    def __repr__(self):
        return 'Update ' + self.name + ' from ' + self.client

class Checkout:
    def __init__(self, client, name, file=None, timestamp=None, ids=None):
        self.code = 6
        self.client = client
        self.name = name
        self.file = file
        self.timestamp = timestamp
        self.ids = ids
    def __repr__(self):
        return 'Checkout ' + self.name + ' from ' + self.client + ' on ' + self.timestamp

class executeDaemon(Thread):
    def __init__(self, server):
        Thread.__init__(self)
        self.server = server
        self.daemon = True
        self.start()

    def run(self):
        while True:
            # print('daemon!!!: ' + str(self.server.coord))
            if(self.server.coord and self.server.getServerID() == self.server.coord['id']):
                run_coord(self.server, self.server.getHOST(), self.server.getPORT(),0)

class executeController(Thread):
    def __init__(self, k, broadcaster):
        Thread.__init__(self)
        self.daemon = True
        self.server = VersionController(get_ip_address(), 9091, k, broadcaster)
        self.start()

    def run(self):
        #setAvailablePORT(self.server,self.server.getPORT())
        print('POOOOORT ' + str(self.server.getPORT()))
        self.server.getID()
        self.ip = get_ip_address()
        while not self.server.coord:
            pass
        print(self.server.coord)
        executeDaemon(self.server)


class receiverProcesser(Thread):
    def __init__(self, receiver, broadcaster, server):
        Thread.__init__(self)
        self.daemon = True
        self.receiver = receiver
        self.broadcaster = broadcaster
        self.server = server
        self.start()

    def run(self):
        # Caso cuando recibo mensajes: de coordinador, eleccion, o are you alive
        while True:
            if(self.receiver.theresMessage()):
                message = self.receiver.getQueuedMessage()
                if(message.code == 0):
                    self.server.serversTable[message.id] = message.ip + ':' + str(message.port)
                    print(self.server.serversTable)
                elif(message.code == 1):
                    self.broadcaster.setMessage(ElectionMessage(self.server.getServerID()))
                    self.broadcaster.canSend()
                elif(message.code == 2):
                    self.server.coord = {
                        'id':message.id,
                        'ip':message.ip,
                        'port':message.port
                    }
                    print('received coord msg!!!!!!!!!!!!!!!!!!!!!!!!!')
                elif(message.code == 4):
                    self.server.versionTable = message.versionTable
                    self.server.serversTable = message.serversTable
                    print("received heartbeat and \nversion table "+str(message.versionTable)+"\nservers table "+str(message.serversTable))

class broadcasterProcesser(Thread):
    def __init__(self, broadcaster, server):
        Thread.__init__(self)
        self.electionResponses = 0
        self.heartbeats = []
        self.daemon = True
        self.broadcaster = broadcaster
        self.server = server
        self.start()

    def run(self):
        # Caso cuando me responde al broadcast y/o termina el broadcast 
        while True:
            if(self.broadcaster.theresMessage()):
                response = self.broadcaster.getQueuedMessage()
                # print('estoy en el primer if')
                print(response)
                if(response.code == 0):
                    self.server.serversTable[response.id] = response.ip + ':' + str(response.port)
                    print('SERVERS TABLE')
                    print(self.server.serversTable)
                elif(response.code == 2):
                    print('received coord msg, debo cambiar controller.server.coord')
                    self.server.coord = {
                        'id': response.id,
                        'ip': response.ip,
                        'port': response.port
                    }
                    print('servercoord : ' + str(self.server.coord))
                elif(response.code == 3):
                    print('ack response to ' + str(response.responseTo))  
                    self.electionResponses += 1
                    # Debo contar este numero de responses
                    #TODO: Debo preguntar si termine la transmision del id para verificar si hay coordinador,
                    # Si no hay => comienzo eleccion, seria broadcast => can send y el mensaje es de eleccion
                    # en receive si recibo un mensaje de eleccion con un id menor lo ignoro
                elif(response.code  == 4):
                    print('received heartbeat and el coord es: ' + str(self.server.coord))
                    print('LISTA DE HEARTBEATS::::::: ' + str(response))
                    self.heartbeats.append(response.id)
            elif(self.broadcaster.getEndTransmission()['endTransmission']):
                # Si termino la transmision y ya procese todos los mensajes
                print('ELECTION RESPONSES: ' + str(self.electionResponses))
                if(self.broadcaster.getEndTransmission()['messageType'] == 0):
                    if(not self.server.coord):
                        serverID = self.server.getServerID()
                        message = ElectionMessage(serverID)
                        self.broadcaster.setMessage(message)
                        self.broadcaster.canSend()
                        print('inicio election')
                elif(self.broadcaster.getEndTransmission()['messageType'] == 1):
                    if(self.electionResponses == 0):
                        self.server.coord = {
                            'id': self.server.getServerID(),
                            'ip': self.server.getHOST(),
                            'port': self.server.getPORT()
                        }
                        # run_coord(self.server, self.server.getHOST(), self.server.getPORT(),0)
                        self.broadcaster.setMessage(CoordMessage(self.server.coord['id'],self.server.coord['ip'],self.server.coord['port']))
                        self.broadcaster.canSend()
                    else:
                        self.electionResponses = 0
                    print('fin mensaje de eleccion, debo contar los ack')
                    print('COORD')
                    print(self.server.coord)
                elif(self.broadcaster.getEndTransmission()['messageType'] == 4):
                    print('process hearbeats!!!!!')
                    print(self.server.serversTable.items())
                    print(self.heartbeats)
                    if(len(self.heartbeats) < len(self.server.serversTable)):
                        print('LEN MENOR')
                        serversTableKeys = list(self.server.serversTable)
                        print('keys!!!!!!: ' + str(serversTableKeys))
                        print('HEARTBEATS!!!!!: ' + str(self.heartbeats))
                        #print(self.heartbeats)
                        for key in serversTableKeys:
                            print(key)
                            if(key not in self.heartbeats):
                                self.server.heartbeats += 1
                                if(self.server.heartbeats > 5):
                                    # key_version = self.server.serversTable[key]
                                    del self.server.serversTable[key]
                                    print('Deleted : ' + str(key))
                                    if key in self.server.versionTable:
                                        del self.server.versionTable[key]
                                    #self.server.partitionTable = calcPartitions(self.server.serversTable, self.server.coord['id'], self.server.k)
                            else:
                                self.server.hearbeats = 0
                    self.heartbeats = []
                self.broadcaster.setEndTransmission(False)
            # Los casos de enviar mensajes dado un mensaje en especifico se manejan en receiver y broadcast
            # Los casos de hacer algun proc con el servidor se manejan en estos hilos
            # Hilos nuevos: broadcasterProcesser, receiverProcesser                

class receiverServiceProcesser(Thread):
    def __init__(self, receiver, broadcaster, server):
        Thread.__init__(self)
        self.daemon = True
        self.receiver = receiver
        self.broadcaster = broadcaster
        self.server = server
        self.start()

    def run(self):
        # Caso cuando recibo mensajes: de coordinador, eleccion, o are you alive
        while True:
            if(self.receiver.theresMessage()):
                message = self.receiver.getQueuedMessage()
                if(message.code == 0):
                    self.server.serversTable[message.id] = message.ip + ':' + str(message.port)
                    print(self.server.serversTable)
                elif(message.code == 1):
                    self.broadcaster.setMessage(ElectionMessage(self.server.getServerID()))
                    self.broadcaster.canSend()
                elif(message.code == 2):
                    self.server.coord = {
                        'id':message.id,
                        'ip':message.ip,
                        'port':message.port
                    }
                    print('received coord msg!!!!!!!!!!!!!!!!!!!!!!!!!')
                elif(message.code == 4):
                    self.server.versionTable = message.versionTable
                    self.server.serversTable = message.serversTable
                    print("received heartbeat and \nversion table "+str(message.versionTable)+"\nservers table "+str(message.serversTable))

class broadcasterServiceProcesser(Thread):
    def __init__(self, broadcaster, server):
        Thread.__init__(self)
        self.electionResponses = 0
        self.heartbeats = []
        self.daemon = True
        self.broadcaster = broadcaster
        self.server = server
        self.start()

    def run(self):
        # Caso cuando me responde al broadcast y/o termina el broadcast 
        while True:
            if(self.broadcaster.theresMessage()):
                response = self.broadcaster.getQueuedMessage()
                # print('estoy en el primer if')
                print(response)
                if(response.code == 0):
                    self.server.serversTable[response.id] = response.ip + ':' + str(response.port)
                    print('SERVERS TABLE')
                    print(self.server.serversTable)
                elif(response.code == 2):
                    print('received coord msg, debo cambiar controller.server.coord')
                    self.server.coord = {
                        'id': response.id,
                        'ip': response.ip,
                        'port': response.port
                    }
                    print('servercoord : ' + str(self.server.coord))
                elif(response.code == 3):
                    print('ack response to ' + str(response.responseTo))  
                    self.electionResponses += 1
                    # Debo contar este numero de responses
                    #TODO: Debo preguntar si termine la transmision del id para verificar si hay coordinador,
                    # Si no hay => comienzo eleccion, seria broadcast => can send y el mensaje es de eleccion
                    # en receive si recibo un mensaje de eleccion con un id menor lo ignoro
                elif(response.code  == 4):
                    print('received heartbeat and el coord es: ' + str(self.server.coord))
                    self.heartbeats.append(response.id)
            elif(self.broadcaster.getEndTransmission()['endTransmission']):
                # Si termino la transmision y ya procese todos los mensajes
                print('ELECTION RESPONSES: ' + str(self.electionResponses))
                if(self.broadcaster.getEndTransmission()['messageType'] == 0):
                    if(not self.server.coord):
                        serverID = self.server.getServerID()
                        message = ElectionMessage(serverID)
                        self.broadcaster.setMessage(message)
                        self.broadcaster.canSend()
                        print('inicio election')
                elif(self.broadcaster.getEndTransmission()['messageType'] == 1):
                    if(self.electionResponses == 0):
                        self.server.coord = {
                            'id': self.server.getServerID(),
                            'ip': self.server.getHOST(),
                            'port': self.server.getPORT()
                        }
                        # run_coord(self.server, self.server.getHOST(), self.server.getPORT(),0)
                        self.broadcaster.setMessage(CoordMessage(self.server.coord['id'],self.server.coord['ip'],self.server.coord['port']))
                        self.broadcaster.canSend()
                    else:
                        self.electionResponses = 0
                    print('fin mensaje de eleccion, debo contar los ack')
                    print('COORD')
                    print(self.server.coord)
                elif(self.broadcaster.getEndTransmission()['messageType'] == 4):
                    print('process hearbeats!!!!!')
                    print(self.server.serversTable.items())
                    print(self.heartbeats)
                    if(len(self.heartbeats) < len(self.server.serversTable)):
                        print('LEN MENOR')
                        serversTableKeys = list(self.server.serversTable)
                        print(serversTableKeys)
                        print(self.heartbeats)
                        for key in serversTableKeys:
                            print(key)
                            if(key not in self.heartbeats):
                                self.server.heartbeats += 1
                                if(self.server.heartbeats > 5):
                                    # key_version = self.server.serversTable[key]
                                    del self.server.serversTable[key]
                                    del self.server.versionTable[key]
                                    self.server.partitionTable = calcPartitions(self.server.serversTable, self.server.coord['id'], self.server.k)
                            else:
                                self.server.hearbeats = 0
                    self.heartbeats = []
                self.broadcaster.setEndTransmission(False)
            # Los casos de enviar mensajes dado un mensaje en especifico se manejan en receiver y broadcast
            # Los casos de hacer algun proc con el servidor se manejan en estos hilos
            # Hilos nuevos: broadcasterProcesser, receiverProcesser                


class heartbeatSender(Thread):
    def __init__(self, broadcaster, server):
        Thread.__init__(self)
        self.daemon = True
        self.broadcaster = broadcaster
        self.server = server
        self.start()

    def run(self):
        # are you alive 
        while not self.server.coord:
            pass
        while True:
                if(self.server.coord and self.server.coord['id'] == self.server.getServerID()):
                    print('sending heartbeat')
                    time.sleep(3)
                    self.broadcaster.setMessage(Heartbeat(serversTable=self.server.serversTable, versionTable=self.server.versionTable))
                    self.broadcaster.canSend()
                    

class heartbeatChecker(Thread):
    def __init__(self, broadcaster,receiver, server):
        Thread.__init__(self)
        self.daemon = True
        self.broadcaster = broadcaster
        self.receiver = receiver
        self.server = server
        self.start()

    def run(self):
        # are you alive 
        while not self.server.coord:
            pass
        while True:
                if(self.server.coord and self.server.coord['id'] != self.server.getServerID()):
                    time.sleep(30)
                    print('waitinggg')
                    if(not self.receiver.heartbeatReceived and self.server.coord['id']):
                        print(self.server.coord)
                        print(self.server.serversTable)
                        del self.server.serversTable[self.server.coord['id']] 
                        self.server.coord['id'] = None
                        self.broadcaster.messageQueue = []
                        self.receiver.messageQueue = []
                        self.broadcaster.setMessage(ElectionMessage(self.server.getServerID()))
                        self.broadcaster.canSend()
                    else:
                        self.receiver.heartbeatReceived = False
                        
class replicateReceiver(Thread):
    def __init__(self,server):
        Thread.__init__(self)
        self.daemon = True
        self.server = server
        self.start()

    def run(self):

        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        newsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


        # Bind the socket to the port
        server_address = (str(self.server.host), 20000)
        # print('Starting up on {} port {}'.format(*server_address))
        sock.bind(server_address)
        sock.listen(1)

        while not self.server.coord:
            pass
        
        if self.server.coord['id']==self.server.id:
            nextReplicateServer = getNextReplicateServer(self.server.lastReplicateServer, self.server.serversTable, self.server.coord['id'])
            self.server.lastReplicateServer = nextReplicateServer
        else:
            nextReplicateServer = getNextReplicateServer(self.id, self.serversTable)
        
        print(self.server.serversTable[nextReplicateServer].split(':')[0])
        next_server_address = (str(self.server.serversTable[nextReplicateServer].split(':')[0]), 20001)
        newsocket.bind(next_server_address)
                    

        # Listen for incoming connections

        while True:
            # Wait for a connection
            print('waiting for a connection')
            connection, client_address = sock.accept()
            try:
                print('connection from', client_address)

                while True:
                    data = connection.recv(4096)
                    obj = pickle.loads(data)
                    print(obj)
                    time.sleep(10)

                    print('sending {!r}'.format(data))
                    encoded_data = pickle.dumps(data)
                    newsocket.sendall(encoded_data)

                    if data:
                        connection.sendall(data)
                    else:
                        print('no data from', client_address)
                        break

            finally:
                # Clean up the connection
                print("Closing current connection")
                connection.close()

class replicateSender(Thread):
    def __init__(self,server):
        Thread.__init__(self)
        self.daemon = True
        self.server = server
        self.start()

    def run(self):

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect the socket to the port where the server is listening
        server_address = (str(self.server.host), 20000)
        # print('connecting to {} port {}'.format(*server_address))
        sock.connect(server_address)

        try:

            # Send data
            data = {
                'file': "sirve",
                'name': "vamos",
                'id': "porfavor"
            }

            print('sending {!r}'.format(data))
            encoded_data = pickle.dumps(data)
            sock.sendall(encoded_data)

            # Look for the response
            amount_received = 0
            amount_expected = len(encoded_data)

            while amount_received < amount_expected:
                data = sock.recv(100)
                amount_received += len(encoded_data)
                print('received {!r}'.format(encoded_data))

        finally:
            print('closing socket')
            sock.close()
        


if __name__ == "__main__":
    serviceBroadcaster = broadcastService()
    k = 1
    controller = executeController(k, serviceBroadcaster)
    serviceReceiver = receiveService(controller.server)
    print("started controller")
    receiver = receive(controller.server)
    broadcaster = broadcast(controller.server)
    # time.sleep(3)
    # replicateReceiver(controller.server)
    # replicateSender(controller.server)
    # time.sleep(3)
    print("started multicast sender")
    receiverProcesser = receiverProcesser(receiver,broadcaster,controller.server)
    broadcasterProcesser = broadcasterProcesser(broadcaster,controller.server)
    heartbeatSender = heartbeatSender(broadcaster,controller.server)
    heartbeatChecker = heartbeatChecker(broadcaster,receiver,controller.server)
    # si soy coordinador => activo el hilo de ejecucion de coordinador que iniciara aca
    while True:
        if(controller.server.getServerID() and controller.server.getStartingValue()):
            print(controller.server.getServerID())
            # Debo enviar un obj con: id del mensaje - 0, id del server, ip y puerto
            # Set server info
            serverID = controller.server.getServerID()
            serverHOST = controller.server.getHOST()
            serverPORT = controller.server.getPORT()
            # Server info es para respuestas a broadcasts de ids
            receiver.setServerInfo(serverID,serverHOST,serverPORT)
            # Mensaje para enviar como broadcast
            message = IdMessage(serverID,serverHOST,serverPORT)
            broadcaster.setMessage(message)
            broadcaster.canSend()
            # agregar valor running para saber si puedo hacer una eleccion 
            controller.server.setRunning()
                

