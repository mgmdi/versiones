import Pyro4
import netifaces as ni
from datetime import datetime
from collections import defaultdict
import threading
import time

@Pyro4.expose
class VersionController(object):

    def __init__(self):
        self.files = {}
        self.table = ""
    
    # Services
    def commit(self, file, name, id):
        self.addFile(file, name, id)

    def checkout(self, name, id, time):
        key = name + ':' + id
        if(key in self.files):
            for version in self.files[key]:
                print(version)
                date_time_obj = datetime.strptime(time, '%m/%d/%Y %H:%M:%S')
                stamp = datetime.timestamp(date_time_obj)
                print(stamp)
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
            for version in self.files[key]:
                date = datetime.fromtimestamp(version['timestamp'])
                date_time = date.strftime('%m/%d/%Y %H:%M:%S')
                versions['datetime'] = date_time
                versions['file'] = version['file']
        return versions

    def addFile(self, file, name, id):
        now = datetime.now()
        date_time = now.strftime('%m/%d/%Y %H:%M:%S')
        print('Date time: ' + name + ' ' + date_time)
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
        ip = ni.ifaddresses('wlo1')[ni.AF_INET][0]['addr']
        servers = find_servers()
        servers.saveTable("holi ")
        time.sleep(5)

threads = list()
if __name__ == "__main__":
    server = VersionController()
    ip = ni.ifaddresses('wlo1')[ni.AF_INET][0]['addr']
    # Establecer un puerto del sistema
    #hilo para hacer broadcast
    t = threading.Thread(target=broadcast)
    threads.append(t)
    t.start()
    with Pyro4.Daemon(host=ip,port=9091) as daemon:
        server_uri = daemon.register(server)
        with Pyro4.locateNS() as ns:
            ns.register("server.test2", server_uri)
        print("Servers available.")
        daemon.requestLoop()