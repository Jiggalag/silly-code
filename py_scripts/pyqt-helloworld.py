#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
from PyQt5.QtWidgets import QApplication, QWidget, QCheckBox, QInputDialog, QLineEdit, QPushButton
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import pyqtSlot

class Example(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setGeometry(300, 300, 300, 220)
        self.setWindowTitle('Test')
        self.setWindowIcon(QIcon('./resources/av.jpg'))

        cb_enableSchemaChecking = QCheckBox('Compare schema', self)
        cb_enableSchemaChecking.move(20, 20)
        cb_enableSchemaChecking.toggle()
        cb_enableSchemaChecking.stateChanged.connect(self.enableSchemaCheckingConstruct)

        cb_failWithFirstError = QCheckBox('Only first error', self)
        cb_failWithFirstError.move(20, 40)
        cb_failWithFirstError.toggle()
        cb_failWithFirstError.stateChanged.connect(self.failWithFirstErrorConstruct)

        cb_mode = QCheckBox('Checking type', self)
        cb_mode.move(20, 60)
        cb_mode.toggle()
        cb_mode.stateChanged.connect(self.modeConstruct)

        btn_setConfiguration = QPushButton('Set configuration', self)
        btn_setConfiguration.setToolTip('This is an example button')
        btn_setConfiguration.move(160, 180)
        btn_setConfiguration.clicked.connect(self.on_click)

        self.show()


    def enableSchemaCheckingConstruct(self, state):
        if state == Qt.Checked:
            self.enableSchemaChecking = True
        else:
            self.enableSchemaChecking = False


    def failWithFirstErrorConstruct(self, state):
        if state == Qt.Checked:
            self.failWithFirstError = True
        else:
            self.failWithFirstError = False


    def modeConstruct(self, state):
        if state == Qt.Checked:
            self.mode = True
        else:
            self.mode = False


    @pyqtSlot()
    def setConfiguration(self):
        print('Set configuration')


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = Example()
    sys.exit(app.exec_())