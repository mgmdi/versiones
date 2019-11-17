import Pyro4
import netifaces as ni
from datetime import datetime
from collections import defaultdict

@Pyro4.expose
class VersionController(object):

    def __init__(self):
        self.files = {}
    
    # Services
    def commit(self, file, name, id):
        self.addFile(file, name, id)

    def checkout(self, name, id, time):
        key = name + ':' + id
        if(key in self.files):
            for version in self.files[key]:
                date_time_obj = datetime.datetime.strptime(time, '%m/%d/%Y %H:%M:%S')
                timestamp = date_time_obj.timestamp("%m/%d/%Y %H:%M:%S")
                if(version['timestamp'] == timestamp):
                    print(version)
                    return version
        return {}     
        
    def update(self):
        print('This is update')

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
        timestamp = datetime.timestamp(now)
        fileInfo = {'file': file, 'timestamp': timestamp}
        index = name + ':' + id
        if index in self.files:
            self.files[index].append(fileInfo)
        else:
            self.files[index] = [fileInfo]
        


if __name__ == "__main__":
    server = VersionController()
    ip = ni.ifaddresses('wlo1')[ni.AF_INET][0]['addr']
    # Establecer un puerto del sistema
    with Pyro4.Daemon(host=ip,port=9090) as daemon:
        server_uri = daemon.register(server)
        with Pyro4.locateNS() as ns:
            ns.register("server.test", server_uri)
        print("Servers available.")
        daemon.requestLoop()