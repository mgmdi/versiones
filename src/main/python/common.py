import netifaces as ni

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