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


@Pyro4.expose
class VersionController(object):

    def __init__(self):
        self.files = {}
        self.id = None

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
        config = open(
            op.join(op.dirname(op.abspath(__file__)), "config.txt"), "r")
        HOST = config.readline().strip('\n')    # The remote host
        # The same port as used by the server
        PORT = int(config.readline().strip('\n'))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            data = s.recv(1024)
            self.id = data.decode()
        print('Received', repr(data))


class receive(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.start()
    
    def run(self):
        multicast_group = '224.10.10.10'

        server_address = ('', 10000)

        # Create the socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Bind to the server address
        sock.bind(server_address)

        # Tell the operating system to add the socket to
        # the multicast group on all interfaces.
        group = socket.inet_aton(multicast_group)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        sock.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            mreq)

        # Receive/respond loop
        while True:
            print('\nwaiting to receive message')
            data, address = sock.recvfrom(1024)

            print('received {} bytes from {}'.format(
                len(data), address))
            print(data)

            print('sending acknowledgement to', address)
            sock.sendto(b'ack', address)


class executeController(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.start()

    def run(self):
        server = VersionController()
        ip = get_ip_address()
        # Establecer un puerto del sistema
        # with Pyro4.Daemon(host=ip, port=9091) as daemon:
        #     server_uri = daemon.register(server)
        #     with Pyro4.locateNS() as ns:
        #         ns.register("server.test", server_uri)
        #     # Debo pedir mi id
        #     server.getID()
        #     print("Servers available.")
        #     daemon.requestLoop()
        run_server(server, ip, 9091, 0)

if __name__ == "__main__":
    # versionController = threading.Thread(target=executeController())
    # versionController.start()
    # broadcastReceiver = threading.Thread(target=receive())
    # broadcastReceiver.start()
    executeController()
    print("started controller")
    receive()
    print("started multicast receiver")
    while True:
        pass
"""     time.sleep(2)
    print('send')
    broadcastSender = threading.Thread(target=broadcast())
    broadcastSender.start() """
