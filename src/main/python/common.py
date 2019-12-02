import netifaces as ni
import Pyro4
import datetime
import time
from urllib.request import urlopen

def get_ip_address():
    # Find interface with assigned ip address
    interfaces = ni.interfaces()
    ip = ""
    for interface in interfaces:
        if interface=='lo':
            continue
        try:
            ip = ni.ifaddresses(interface)[ni.AF_INET][0]['addr']
            break
        except:
            continue
    if ip=="":
        return None
    return str(ip)

def run_server(server, ip, server_port, server_no):
    connected = False
    while(not connected):
        try:
            with Pyro4.Daemon(host=ip, port=server_port) as daemon:
                server_uri = daemon.register(server)
                with Pyro4.locateNS() as ns:
                    ns.register(f"server.test{server_no}", server_uri)
                # Debo pedir mi id
                server.getID()
                print("Servers available.")
                connected = True
                daemon.requestLoop()
        except: # TODO: AVERIGUAR CUAL ES LA EXCEPCION PARA ABORTAR EN LAS OTRAS
            server_port += 1
            server_no += 1


def get_utc_time():
    res = urlopen('http://just-the-time.appspot.com/')
    result = res.read().strip()
    result_str = result.decode('utf-8')
    x = result_str.split(" ")
    x1 = x[0].split("-")
    date = x1[1] + "/" + x1[2] + "/" + x1[0] + " " + x[1]
    timestamp = time.mktime(datetime.datetime.strptime(date, '%m/%d/%Y %H:%M:%S').timetuple())
    return timestamp
