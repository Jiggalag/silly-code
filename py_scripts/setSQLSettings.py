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

        btn_save = QPushButton('Save', self)
        btn_save.setToolTip('Save button')
        btn_save.move(160, 180)
        btn_save.clicked.connect(self.saveConfiguration)

        self.show()

    @pyqtSlot()
    def saveConfiguration(self):
        print('Set configuration')