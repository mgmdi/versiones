import subprocess
import threading
import socket
from common import get_ip_address


class NameServer:

    def NS(self):
        ip = get_ip_address()
        if ip==None:
            print("Not connected to the internet")            
            return
        subprocess.call(
            "python -m Pyro4.naming --host "+ip+" --port 8080", shell=True)


class Server(object):

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self.mutex = threading.Lock()
        self.ids = 0

    def getID(self):
        self.mutex.acquire()
        serverId = self.ids
        self.ids += 1
        self.mutex.release()
        return serverId

    def IDAssignation(self):
        HOST = self._host         # Symbolic name meaning all available interfaces
        PORT = self._port         # Arbitrary non-privileged port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((HOST, PORT))
            s.listen(10)
            while True:
                conn, addr = s.accept()
                with conn:
                    print('Connected by', addr)
                    conn.sendall(str(self.getID()).encode())


def execute():
    # Name server
    nameServer = NameServer()
    server_ip = get_ip_address()
    config = str(server_ip) + "\n9090"
    g = open("config.txt","w+")
    g.write(config)
    g.close()
    if server_ip!=None:
        idAssignation = Server(server_ip, 9090)
    else:
        print("Not connected to the internet")
    # Aqui inicio el name server thread y el thread de asignacion
    NS = threading.Thread(target=nameServer.NS)
    NS.start()
    print('started NS')
    idAssignationServer = threading.Thread(target=idAssignation.IDAssignation())
    idAssignationServer.start()
    print('started id assignation server')


if __name__ == "__main__":
    execute()
