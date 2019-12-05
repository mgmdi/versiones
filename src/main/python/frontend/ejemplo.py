
from PyQt5 import QtWidgets, QtGui,QtCore
from mydesing import Ui_MainWindow  # importing our generated file
 
import sys
 
class mywindow(QtWidgets.QMainWindow):

	array = ["1","2","3","4","5"]

	array2 = [["1.1","1.2","1.3"],["2.1","2.2","2.3"],["3.1","3.2","3.3"],["4.1","4.2","4.3"],["5.1","5.2","5.3"]]
 
	def __init__(self):
	 
	    super(mywindow, self).__init__()
	 
	    self.ui = Ui_MainWindow()
	    
	    self.ui.setupUi(self)

	    self.ui.pushButton_2.clicked.connect(self.btnClicked)

	    # Llenamos el array con cosas
	    self.ui.comboBox.addItem("None")
	    self.ui.comboBox.addItems(self.array)
	    self.ui.comboBox.currentIndexChanged.connect(self.selectionChange)


	def btnClicked(self):
		self.ui.label_5.setText("Commit del archivo: " + self.ui.lineEdit.text())

	def selectionChange(self):
		self.ui.label_2.setText("Selecciono la opcion: " + self.ui.comboBox.currentText())
		self.ui.comboBox_2.clear()
		self.ui.comboBox_2.addItems(self.array2[int(self.ui.comboBox.currentText())-1])

    
	 
app = QtWidgets.QApplication([])
 
application = mywindow()
 
application.show()
 
sys.exit(app.exec())