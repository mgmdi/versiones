import Pyro4
import netifaces as ni
import time
from common import get_ip_address
import os.path as op

class Client:

    def __init__(self, id):
        self.id = id

    
def find_servers():

    server = None
    with Pyro4.locateNS() as ns:
        for server, server_uri in ns.list(prefix="server.").items():
            print("found server", server)
            server = Pyro4.Proxy(server_uri)
    if not server:
        raise ValueError("No servers found")
    return server

def main():
    ip = get_ip_address()
    if ip==None:
        print("Not connected to the internet")
        return -1
    client = Client(ip) # Debo obtener la direccion ip para pasarla como parametro
    servers = find_servers()
    versiones_dir = op.abspath(op.join(__file__, op.pardir, op.pardir, op.pardir, op.pardir))
    file = open(op.join(versiones_dir, "requirements.txt"),'r')
    file_ = open(op.join(versiones_dir, "test.txt"),'r')
    servers.commit(file.read(), 'file', ip)
    time.sleep(3.5)
    servers.commit(file_.read(), 'file', ip)
    # servers.getVersions('file', ip)
    # time.sleep(3.5)
    servers.update('file', ip)
    time.sleep(3.5)
    # print(servers.checkout('file', ip, '11/24/2019 22:12:29'))
    # Server getVersions pasandole el cliente y el nombre del archivo para mostrar


if __name__ == "__main__":
    main()