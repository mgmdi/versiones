import Pyro4
import netifaces as ni
from datetime import datetime
from collections import defaultdict
import threading
import time
import socket
from common import get_ip_address
import os.path as op


@Pyro4.expose
class VersionController(object):

    def __init__(self):
        self.files = {}
        self.table = ""
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
        config = open(op.join(op.dirname(op.abspath(__file__)), "config.txt"),"r")
        HOST = config.readline().strip('\n')    # The remote host
        PORT = int(config.readline().strip('\n'))          # The same port as used by the server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            data = s.recv(1024)
            self.id = data.decode()
        print('Received', repr(data))


    def saveTable(self,message):
        self.table += message
        print(self.table)


def find_servers():

    server = None
    with Pyro4.locateNS() as ns:
        for server, server_uri in ns.list(prefix="server.").items():
            print("found server", server)
            server = Pyro4.Proxy(server_uri)
    if not server:
        raise ValueError("No servers found")
    return server

def broadcast():
    while(True):
        servers = find_servers()
        servers.saveTable("holi ")
        time.sleep(5)

threads = list()
if __name__ == "__main__":
    server = VersionController()
    ip = get_ip_address()
    # Establecer un puerto del sistema
    #hilo para hacer broadcast
    t = threading.Thread(target=broadcast)
    threads.append(t)
    t.start()
    # with Pyro4.Daemon(host=ip,port=9091) as daemon:
    #     server_uri = daemon.register(server)
    #     with Pyro4.locateNS() as ns:
    #         ns.register("server.test2", server_uri)
    
    with Pyro4.Daemon(host=ip, port=9091) as daemon:
        server_uri = daemon.register(server)
        with Pyro4.locateNS() as ns:
            ns.register("server.test", server_uri)
        # Debo pedir mi id
        server.getID()
        print("Servers available.")
        daemon.requestLoop()
