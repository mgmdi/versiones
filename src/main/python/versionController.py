import Pyro4
import netifaces as ni

@Pyro4.expose
class VersionController(object):

    def __init__(self):
        self.files = {}
    
    # Services
    def commit(self, file, name):
        self.files[name] = file
        print('This is commit: ' + file)
        print(self.files)

    def checkout(self):
        print('This is checkout')

    def update(self):
        print('This is update')


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