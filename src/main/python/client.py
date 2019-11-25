import Pyro4
import netifaces as ni
import time

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
    ip = ni.ifaddresses('wlo1')[ni.AF_INET][0]['addr']
    client = Client(ip) # Debo obtener la direccion ip para pasarla como parametro
    servers = find_servers()
    file = open('/home/mgmdi/Desktop/Versiones/requirements.txt','r')
    file_ = open('/home/mgmdi/Desktop/Versiones/test.txt','r')
    servers.commit(file.read(), 'file', ip)
    time.sleep(3.5)
    servers.commit(file_.read(), 'file', ip)
    # servers.update('file', ip)
    servers.getVersions('file',ip)
    # servers.checkout('file',ip, '11/17/2019 10:46:23')
    # Server getVersions pasandole el cliente y el nombre del archivo para mostrar


if __name__ == "__main__":
    main()