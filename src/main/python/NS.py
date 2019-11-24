import subprocess
import threading
import socket


class NameServer:


    # TODO: hacer parametrizable la llamada al servidor de nombre

    def NS(self):
        subprocess.call(
            "python -m Pyro4.naming --host 192.168.0.105 --port 8080", shell=True)


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
        PORT = 9090         # Arbitrary non-privileged port
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
    idAssignation = Server('192.168.0.105', 9090)
    print(socket.gethostname())
    # Aqui inicio el name server thread y el thread de asignacion
    NS = threading.Thread(target=nameServer.NS)
    NS.start()
    print('started NS')
    idAssignationServer = threading.Thread(target=idAssignation.IDAssignation())
    idAssignationServer.start()
    print('started id assignacion server')


if __name__ == "__main__":
    execute()
