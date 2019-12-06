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

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.files = {}
        self.id = None
        self.serversTable = {}
        self.starting = True
        self.coord = None
        self.lastReplicateServer = -1 # last server for k-replication

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
        return {}

    def update(self, name, id):
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

    def sendAction(self, file, name, id):
        while not self.coord:
            pass

        # Busca el siguiente servidor para iniciar la replicacion
        upperBoundId = None
        for serverId in serversTable:
            if serverId>self.lastReplicateServer:
                if upperBoundId:
                    if serverId<upperBoundId:
                        upperBoundId = serverId
                else:
                    upperBoundId = serverId

        self.lastReplicateServer=upperBoundId
        ipPortaux = self.serversTable[upperBoundId]
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
        sock.settimeout(0.8)
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
                                self.response = Heartbeat(data.id)
                                print('heartbeat de ' + str(data.id))
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
        self.HOSTID = id

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
            print('\nwaiting to receive message')
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
                if(data.id > self.HOSTID):
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
                self.receivedMessage = Heartbeat()
                self.messageQueue.append(self.receivedMessage)
                sock.sendto(pickle.dumps(Heartbeat(self.server.getServerID())), address)
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
    def __init__(self,id=None):
        self.code = 4
        self.id = id
    def __repr__(self):
        return 'ack'

class executeController(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.server = VersionController(get_ip_address(), 9091)
        self.start()

    def run(self):
        self.server.getID()
        self.ip = get_ip_address()
        while not self.server.coord:
            pass
        print(self.server.coord)
        if(self.server.getServerID() == self.server.coord['id']):
            run_coord(self.server, self.server.getHOST(), self.server.getPORT(),0)
        else:
            print('im a normal server')

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
                    # No hago nada porque ya tengo el heartbeat checker
                    print('received heartbeat')

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
                print('estoy en el primer if')
                print(response)
                if(response.code == 0):
                    self.server.serversTable[response.id] = response.ip + ':' + str(response.port)
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
                        self.broadcaster.setMessage(CoordMessage(self.server.coord['id'],self.server.coord['ip'],self.server.coord['port']))
                        self.broadcaster.canSend()
                    print('fin mensaje de eleccion, debo contar los ack')
                    print('COORD')
                    print(self.server.coord)
                elif(self.broadcaster.getEndTransmission()['messageType'] == 4):
                    if(len(self.heartbeats) < len(self.server.serversTable)):
                        print('LEN MENOR')
                        for key in self.server.serversTable:
                            if(key not in self.heartbeats):
                                del self.server.serversTable[key]
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
        if(self.server.coord['id'] == self.server.getServerID()):
            while True:
                time.sleep(3)
                self.broadcaster.setMessage(Heartbeat())
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
        if(self.server.coord['id'] != self.server.getServerID()):
            while True:
                time.sleep(30)
                print('waitinggg')
                if(not self.receiver.heartbeatReceived):
                    del self.server.serversTable[self.server.coord['id']] 
                    self.server.coord = None
                    self.broadcaster.setMessage(ElectionMessage(self.server.getServerID()))
                    self.broadcaster.canSend()
                else:
                    self.receiver.heartbeatReceived = False
                    
class replicate(Thread):
    def __init__(self,server,k, message):
        Thread.__init__(self)
        self.daemon = True
        self.server = server
        self.k = k
        self.message = message
        self.start()

    def run(self):
        # pasa mensaje y disminuye k
        self.k = self.k-1
        while not self.server.coord:
            pass
    
        # buscar servidor con id mayor a este usando el serversTable?
        idNext= self.server.getServerID() + 1
        ipPortaux = self.server.serversTable[idNext]
        ipPort = ipPortaux.split(':')
        HOST = ipPort[0]
        PORT = ipPort[1]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = {HOST, int(PORT)}
        sock.bind(server_address)
        # Listen for incoming connections
        sock.listen(10)

        while True:

            connection, client_address = sock.accept()
            print('connection from', client_address)

            # Receive the data in small chunks and retransmit it
            while True:
                data = connection.recv(16)
                #aqui se replica
                if data:
                    connection.sendall(data)
        


if __name__ == "__main__":
    controller = executeController()
    print("started controller")
    receiver = receive(controller.server)
    broadcaster = broadcast(controller.server)
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
                

