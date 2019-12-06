import Pyro4
import netifaces as ni
import time
from common import get_ip_address
import os.path as op
from PyQt5 import QtWidgets, QtGui,QtCore
from mydesing import Ui_MainWindow  # importing our generated file
import sys

class Client:

    def __init__(self, id):
        self.id = id

class mywindow(QtWidgets.QMainWindow):

    client_ip = get_ip_address()

    #servers = find_servers()
    servers = "servers"

    array = ["File 1","File 2","File 3","File 4","File 5"]

    array2 = [["11/24/2019 22:12:29"],["11/24/2019 22:12:29"],["11/24/2019 22:12:29"],["11/24/2019 22:12:29"],["11/24/2019 22:12:29"]]
 
    def __init__(self):
     
        super(mywindow, self).__init__()
     
        self.ui = Ui_MainWindow()
        
        self.ui.setupUi(self)

        self.ui.pushButton_2.clicked.connect(self.btnCommit) ## BOTON DE COMMIT
        self.ui.pushButton_3.clicked.connect(self.btnUpdate) ## BOTON DE UPDATE
        self.ui.pushButton_4.clicked.connect(self.btnCheckout) ## BOTON DE CHECKOUT

        # Llenamos el array con cosas
        self.ui.comboBox.addItem("None")
        self.ui.comboBox.addItems(self.array)
        self.ui.comboBox.currentIndexChanged.connect(self.selectionChange)

    def btnUpdate(self):
        self.ui.label_2.setText("Se hizo update del archivo: " + self.ui.comboBox.currentText())
        update(self.ui.comboBox.currentText(),self.client_ip,self.servers)


    def btnCheckout(self):
        self.ui.label_2.setText("Se hizo checkout del archivo: " + self.ui.comboBox.currentText())
        checkout(self.ui.comboBox.currentText(),self.client_ip,self.ui.comboBox_2.currentText(),self.servers)

    def btnCommit(self):
        self.ui.label_5.setText("Commit del archivo: " + self.ui.lineEdit.text())
        # PORFA REVISA LA FUNCION COMMIT A VER

    def selectionChange(self):
        self.ui.label_2.setText("Selecciono la opcion: " + self.ui.comboBox.currentText())
        self.ui.comboBox_2.clear()
        #self.ui.comboBox_2.addItems(self.array2[int(self.ui.comboBox.currentText())-1])
        versiones = servers.getVersions(self.ui.comboBox.currentText(),self.client_ip)
        versiones = versiones.keys()
        versiones_arreglo = []
        for v in versiones:
            versiones_arreglo.append(v)
        self.ui.comboBox_2.addItems(versiones_arreglo)
 
def find_servers():

    server = None
    with Pyro4.locateNS() as ns:
        for server, server_uri in ns.list(prefix="server.").items():
            print("found server", server)
            server = Pyro4.Proxy(server_uri)
    if not server:
        raise ValueError("No servers found")
    return server

def update(file_name, ip,servers):
    #servers.update(file_name, ip)
    print("Update")

def checkout(file_name,ip,date,servers):
    #servers.checkout(file_name, ip, date)
    print("Checkout")

def commit(file_name,ip):
    versiones_dir = op.abspath(op.join(__file__, op.pardir, op.pardir, op.pardir, op.pardir))
    file = open(op.join(versiones_dir, "requirements.txt"),'r')
    file_ = open(op.join(versiones_dir, "test.txt"),'r')
    servers.commit(file.read(), 'file', ip)
    time.sleep(3.5)
    servers.commit(file_.read(), 'file', ip)

def main():
    ip = get_ip_address()
    if ip==None:
        print("Not connected to the internet")
        return -1
    client = Client(ip) # Debo obtener la direccion ip para pasarla como parametro
    
    
    # COSAS DE LA INTERFAZ

    app = QtWidgets.QApplication([])
     
    application = mywindow()
     
    application.show()
     
    sys.exit(app.exec())



    # servers.getVersions('file', ip)
    # time.sleep(3.5)

    #time.sleep(3.5)
    # print(servers.checkout('file', ip, '11/24/2019 22:12:29'))
    # Server getVersions pasandole el cliente y el nombre del archivo para mostrar


if __name__ == "__main__":
    main()