import netifaces as ni
import Pyro4
from datetime import datetime
import time
import socket
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

def setAvailablePORT(server,server_port):
    connected = False
    port = server_port
    s = socket.socket()
    while(not connected):
        try:
            s.connect((server.getHOST(), port))
            print('connectedd')
            #connected = True

        except: # TODO: AVERIGUAR CUAL ES LA EXCEPCION PARA ABORTAR EN LAS OTRAS
            print('PORTT')
            print(port)
            port += 1
        finally:
            server.setPORT(port)
            s.close()
            break
        
def run_coord(server, ip, server_port, server_no):
    with Pyro4.Daemon(host=ip, port=server_port) as daemon:
        server_uri = daemon.register(server)
        with Pyro4.locateNS() as ns:
            ns.register(f"server.test{server_no}", server_uri)
            #server.setPORT(server_port)
            print("Servers available.")
            daemon.requestLoop()


def get_utc_time():
    res = urlopen('http://just-the-time.appspot.com/')
    result = res.read().strip()
    result_str = result.decode('utf-8')
    x = result_str.split(" ")
    x1 = x[0].split("-")
    date = x1[1] + "/" + x1[2] + "/" + x1[0] + " " + x[1]
    timestamp = time.mktime(datetime.strptime(date, '%m/%d/%Y %H:%M:%S').timetuple())
    return timestamp


def getNextReplicateServer(lastReplicateServer, serversTable, coordId):
    # Find server for replication
    boundId = None
    for serverId in serversTable:
        if serverId==coordId:
            continue
        if serverId>lastReplicateServer:
            if boundId:
                if serverId<boundId: # Lowest upper bound
                    boundId = serverId
            else:
                boundId = serverId
    
    if not boundId: # We have to restart list and get min
        for serverId in serversTable:
            if serverId==coordId:
                continue
            if serverId<lastReplicateServer:
                if boundId:
                    if serverId<boundId: # Lower bound
                        boundId = serverId
                else:
                    boundId = serverId
    return boundId

def calcPartitions(serversTable, coordId, k):
    partitionTable = {}
    id = 0
    k_aux = k + 1
    for serverId in serversTable:
        if serverId == coordId:
            continue
        partitionTable[id] = []
        server = serverId
        while k_aux!=0:
            partitionTable[id].append(server)
            server = getNextReplicateServer(serverId, serversTable, coordId)
            k_aux -= 1
        k_aux = k + 1
        id += 1
    return partitionTable