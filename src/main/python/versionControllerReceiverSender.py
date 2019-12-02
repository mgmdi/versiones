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
        date_time = now.strftime('%m/%d/%Y %H:%M:%S')
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


class broadcast(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.response = None
        self.message = None
        self.replied = False
        self.send = False
        self.start()

    def canSend(self):
        self.send = True
    
    def cantSend(self):
        self.send = False

    def setMessage(self, msg):
        self.message = pickle.dumps(msg)
    
    def getResponse(self):
        return self.response

    def clearResponse(self):
        self.response = None
    
    def getReplied(self):
        return self.replied

    def setReplied(self, value):
        self.replied = value

    def run(self):
        # message = b'very important data'
        multicast_group = ('224.10.10.10', 10000)

        # Create the datagram socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Set a timeout so the socket does not block
        # indefinitely when trying to receive data.
        sock.settimeout(0.6)

        # Set the time-to-live for messages to 1 so they do not
        # go past the local network segment.
        ttl = struct.pack('b', 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

        while True:
            # Debo setear que no pueda enviar despues recibir los ack -> depende del numero del mensaje recibo n acks
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
                                self.response = {
                                                'code': data.code,
                                                'id': data.id,
                                                'ip': data.ip,
                                                'port': data.port
                                                }
                            self.setReplied(True)
                                # Si recibo con un cod 0, solo lo guardo en la tabla
                        except socket.timeout:
                            print('timed out, no more responses')
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

    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.serverInfo = None
        self.received = False
        self.receivedMsg = None
        self.start()

    def getMessage(self):
        return self.receivedMsg

    def setServerInfo(self, id, ip, port):
        self.serverInfo = IdMessage(id, ip, port)

    def clearMessage(self):
        self.receivedMsg = None

    def getReceived(self):
        return self.received

    def setReceived(self, value):
        self.received = value

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
                self.receivedMsg = {
                    'code': data.code,
                    'id': data.id,
                    'ip': data.ip,
                    'port': data.port
                }
                print('sending server info to', address)
                # Si en el receive recibo un codigo 0 => respondo con la info del server
                sock.sendto(pickle.dumps(self.serverInfo), address)
            else:
                print('sending acknowledgement to', address)
                sock.sendto(b'ack', address)
            self.setReceived(True)
            #self.received = True
            

class IdMessage:
    def __init__(self, id, ip, port):
        self.code = 0
        self.id = id
        self.ip = ip
        self.port = port

    def __repr__(self):
        return 'Server ID: ' + self.id + ' ' + 'IP: ' + self.ip

class executeController(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.server = VersionController(get_ip_address(), 9091)
        self.start()

    def run(self):
        self.ip = get_ip_address()
        run_server(self.server, self.server.getHOST(), self.server.getPORT(),0)


if __name__ == "__main__":
    controller = executeController()
    print("started controller")
    receiver = receive()
    broadcaster = broadcast()
    print("started multicast sender")
    # si soy coordinador => activo el hilo de ejecucion de coordinador que iniciara aca
    # Aqui podemos poner el hilo principal de ejecucion => es el que controla la ejecucion
    while True:
        if(controller.server.getServerID() and controller.server.getStartingValue()):
            print(controller.server.getServerID())
            # Debo enviar un obj con: id del mensaje - 0, id del server, ip y puerto
            # Set server info
            serverID = controller.server.getServerID()
            serverHOST = controller.server.getHOST()
            serverPORT = controller.server.getPORT()
            receiver.setServerInfo(serverID,serverHOST,serverPORT)
            message = IdMessage(serverID,serverHOST,serverPORT)
            broadcaster.setMessage(message)
            broadcaster.canSend()
            # agregar valor running para saber si puedo hacer una eleccion 
            controller.server.setRunning()
        # Caso cuando recibo un id
        if(receiver.getReceived()):
            message = receiver.getMessage()
            if(message['code'] == 0):
                controller.server.serversTable[message['id']] = message['ip'] + ':' + str(message['port'])
                print(controller.server.serversTable)
                receiver.setReceived(False)
                receiver.clearMessage()
        # Caso cuando me responden mi broadcast con un id (unicast)
        if(broadcaster.getReplied()):
            response = broadcaster.getResponse()
            if(response['code'] == 0):
                controller.server.serversTable[message['id']] = message['ip'] + ':' + str(message['port'])
                print(controller.server.serversTable)
                broadcaster.setReplied(False)
                broadcaster.clearResponse()
        
