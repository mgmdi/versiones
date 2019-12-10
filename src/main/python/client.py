import Pyro4
import Pyro4.util
import netifaces as ni
import time
from common import get_ip_address
import os.path as op
from PyQt5 import QtWidgets, QtGui,QtCore
from mydesing import Ui_MainWindow  # importing our generated file
import sys
import base64

class Client:

    def __init__(self, id):
        self.id = id

class mywindow(QtWidgets.QMainWindow):

    client_ip = get_ip_address()
    servers = ""

    def __init__(self,servers):
     
        super(mywindow, self).__init__()

        self.servers = servers
        
        self.ui = Ui_MainWindow()
        
        self.ui.setupUi(self)

        self.ui.pushButton_2.clicked.connect(self.btnCommit) ## BOTON DE COMMIT
        self.ui.pushButton_3.clicked.connect(self.btnUpdate) ## BOTON DE UPDATE
        self.ui.pushButton_4.clicked.connect(self.btnCheckout) ## BOTON DE CHECKOUT
        self.ui.pushButton_5.clicked.connect(self.btnLoad) ## BOTON DE CARGAR ARCHIVOS

        # Llenamos el array con cosas
        self.ui.comboBox.currentIndexChanged.connect(self.selectionChange)
    def btnLoad(self):
        self.ui.comboBox.clear()
        if self.ui.lineEdit_2.text() == "":
            self.ui.label_5.setText("Ingrese un nombre de usuario")
        else:
            self.ui.comboBox.addItem("None")
            files_names = self.servers.getFileNames(self.ui.lineEdit_2.text())
            print("FILES:")
            print(files_names)
            if len(files_names) == 0:
                self.ui.label_5.setText("No hay archivos para este usuario")
            else:
                self.ui.comboBox.addItems(files_names)
                self.ui.label_5.setText("Archivos cargados")

    def btnUpdate(self):
        if self.ui.lineEdit_2.text() == "":
            self.ui.label_5.setText("Ingrese un nombre de usuario")
        else:
            if self.ui.comboBox.currentText()== "None":
                self.ui.label_2.setText("Selecione un archivo")
            else:
                self.ui.label_2.setText("Se hizo update del archivo: " + self.ui.comboBox.currentText())
                update(self.ui.comboBox.currentText(),self.ui.lineEdit_2.text(),self.servers)


    def btnCheckout(self):
        if self.ui.lineEdit_2.text() == "":
            self.ui.label_5.setText("Ingrese un nombre de usuario")
        else:
            if self.ui.comboBox.currentText()== "None":
                self.ui.label_2.setText("Selecione un archivo")
            else:
                self.ui.label_2.setText("Se hizo checkout del archivo: " + self.ui.comboBox.currentText())
                checkout(self.ui.comboBox.currentText(),self.ui.lineEdit_2.text(),self.ui.comboBox_2.currentText(),self.servers)

    def btnCommit(self):
        if self.ui.lineEdit_2.text() == "":
            self.ui.label_5.setText("Ingrese un nombre de usuario")
        else:
            try:
                self.ui.label_5.setText("Commit del archivo: " + self.ui.lineEdit.text())
                commit(self.ui.lineEdit.text(),self.ui.lineEdit_2.text(),self.servers)
                self.ui.lineEdit.setText("")
            except:
                self.ui.label_5.setText("Error con el archivo")

    def selectionChange(self):
        self.ui.label_2.setText("Selecciono la opcion: " + self.ui.comboBox.currentText())
        self.ui.comboBox_2.clear()
        versiones = self.servers.getTimeVersions(self.ui.comboBox.currentText(),self.ui.lineEdit_2.text())
        print("VERSIONES::!:!!:!:")
        print(versiones)
        self.ui.comboBox_2.addItems(versiones)
 
def find_servers():
    server = None
    with Pyro4.locateNS() as ns:
        for server, server_uri in ns.list(prefix="server.").items():
            print("found server", server)
            server = Pyro4.Proxy(server_uri)
    if not server:
        raise ValueError("No servers found")
    return server

def update(file_name, user,servers):
    a = servers.update(file_name, user)
    file = a['file']
    array_file = file_name.split(".")
    if array_file[1] == "txt":
        f=open(file_name,"w")
        f.write(file)
        f.close()
    else:
        data = base64.b64decode(file["data"]) 
        g = open(file_name, "wb")
        g.write(data)
        g.close()

def checkout(file_name,user,date,servers):
    try:
        a = servers.checkout(file_name, user, date)
        print(a)
        file = a['file']
        array_file = file_name.split(".")
        if array_file[1] == "txt":
            f=open(file_name,"w")
            f.write(file)
            f.close()
        else:
            data = base64.b64decode(file["data"]) 
            g = open(file_name, "wb")
            g.write(data)
            g.close()
    except Exception:
        print("Pyro traceback:")
        print("".join(Pyro4.util.getPyroTraceback()))

def commit(file_name,user,servers):
    array_file = file_name.split(".")
    if array_file[1] == "txt":
        file = open(file_name,'r')
        servers.commit(file.read(), file_name, user)
        file.close()
    elif array_file[1] == "png" or array_file[1] == "jpg": 
        with open(file_name, "rb") as img_file:
            img = img_file.read()
            print(img)
            servers.commit(img,file_name,user)


def main():
    f=open("test1.txt","w")
    f.write("my first file\n")
    f.write("This file\n\n")
    f.write("contains three lines\n")
    f.close()
    ip = get_ip_address()
    if ip==None:
        print("Not connected to the internet")
        return -1
    client = Client(ip) # Debo obtener la direccion ip para pasarla como parametro
    
    
    # COSAS DE LA INTERFAZ

    app = QtWidgets.QApplication([])
     
    servers = find_servers()
    application = mywindow(servers)
     
    application.show()
     
    sys.exit(app.exec())



    # servers.getVersions('file', ip)
    # time.sleep(3.5)

    #time.sleep(3.5)
    # print(servers.checkout('file', ip, '11/24/2019 22:12:29'))
    # Server getVersions pasandole el cliente y el nombre del archivo para mostrar


if __name__ == "__main__":
    main()