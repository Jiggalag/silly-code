#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
from PyQt5.QtWidgets import QApplication, QWidget, QCheckBox, QLineEdit, QPushButton, QLabel, QMessageBox, QRadioButton
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import pyqtSlot
import py_scripts.dbComparator.comparatorWithUI as backend

class Example(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setGeometry(0, 0, 900, 600)
        self.setWindowTitle('dbComparator')
        self.setWindowIcon(QIcon('./resources/av.jpg'))

        self.prod_host_label = QLabel('prod.sql-host', self)
        self.prod_host_label.setToolTip('Input host, where prod-db located.\nExample: samaradb03.maxifier.com')
        self.prod_host_label.move(10,25)
        self.prod_host = QLineEdit(self)
        self.prod_host.setToolTip('Input host, where prod-db located.\nExample: samaradb03.maxifier.com')
        self.prod_host.move(160, 20)
        self.prod_host.resize(200, 30)

        self.prod_user_label = QLabel('prod.sql-user', self)
        self.prod_user_label.setToolTip('Input user for connection to prod-db.\nExample: itest')
        self.prod_user_label.move(10,55)
        self.prod_user = QLineEdit(self)
        self.prod_user.setToolTip('Input user for connection to prod-db.\nExample: itest')
        self.prod_user.move(160, 50)
        self.prod_user.resize(200, 30)

        self.prod_password_label = QLabel('prod.sql-password', self)
        self.prod_password_label.setToolTip('Input password for user from prod.sql-user field')
        self.prod_password_label.move(10,85)
        self.prod_password = QLineEdit(self)
        self.prod_password.setToolTip('Input password for user from prod.sql-user field')
        self.prod_password.move(160, 80)
        self.prod_password.resize(200, 30)

        self.prod_db_label = QLabel('prod.sql-db', self)
        self.prod_db_label.setToolTip('Input prod-db name.\nExample: irving')
        self.prod_db_label.move(10,115)
        self.prod_db = QLineEdit(self)
        self.prod_db.setToolTip('Input prod-db name.\nExample: irving')
        self.prod_db.move(160, 110)
        self.prod_db.resize(200, 30)

        self.test_host_label = QLabel('test.sql-host', self)
        self.test_host_label.setToolTip('Input host, where test-db located.\nExample: samaradb03.maxifier.com')
        self.test_host_label.move(500,25)
        self.test_host = QLineEdit(self)
        self.test_host.setToolTip('Input host, where test-db located.\nExample: samaradb03.maxifier.com')
        self.test_host.move(650, 20)
        self.test_host.resize(200, 30)

        self.test_user_label = QLabel('test.sql-user', self)
        self.test_user_label.setToolTip('Input user for connection to test-db.\nExample: itest')
        self.test_user_label.move(500,55)
        self.test_user = QLineEdit(self)
        self.test_user.setToolTip('Input user for connection to test-db.\nExample: itest')
        self.test_user.move(650, 50)
        self.test_user.resize(200, 30)

        self.test_password_label = QLabel('test.sql-password', self)
        self.test_password_label.setToolTip('Input password for user from test.sql-user field')
        self.test_password_label.move(500,85)
        self.test_password = QLineEdit(self)
        self.test_password.setToolTip('Input password for user from test.sql-user field')
        self.test_password.move(650, 80)
        self.test_password.resize(200, 30)

        self.test_db_label = QLabel('test.sql-db', self)
        self.test_db_label.setToolTip('Input test-db name.\nExample: irving')
        self.test_db_label.move(500,115)
        self.test_db = QLineEdit(self)
        self.test_db.setToolTip('Input test-db name.\nExample: irving')
        self.test_db.move(650, 110)
        self.test_db.resize(200, 30)

        self.cb_enableSchemaChecking = QCheckBox('Compare schema', self)
        self.cb_enableSchemaChecking.setToolTip('If you set this option, program will compare also schemas of dbs')
        self.cb_enableSchemaChecking.move(20, 220)
        self.cb_enableSchemaChecking.toggle()
        self.cb_enableSchemaChecking.stateChanged.connect(self.enableSchemaCheckingConstruct)

        self.cb_failWithFirstError = QCheckBox('Only first error', self)
        self.cb_failWithFirstError.setToolTip('If you set this option, comparing will be finished after first error')
        self.cb_failWithFirstError.move(20, 240)
        self.cb_failWithFirstError.toggle()
        self.cb_failWithFirstError.stateChanged.connect(self.failWithFirstErrorConstruct)

        self.btn_setConfiguration = QPushButton('Compare!', self)
        self.btn_setConfiguration.setToolTip('Start comparing of dbs')
        self.btn_setConfiguration.move(750, 550)
        self.btn_setConfiguration.clicked.connect(self.on_click)

        # TODO: add "mode" parameter
        self.mode = QRadioButton("Checking mode")
        self.mode.setChecked(True)
        self.mode.move(500, 500)
        self.mode.toggled.connect(self.on_radio_button_toggled)
        # layout.addWidget(radiobutton, 0, 0)

        self.send_mail_to_label = QLabel('Send mail to', self)
        self.send_mail_to_label.setToolTip('Add one or list of e-mails for receiving results of comparing')
        self.send_mail_to_label.move(10,145)
        self.send_mail_to = QLineEdit(self)
        self.send_mail_to.setToolTip('Add one or list of e-mails for receiving results of comparing')
        self.send_mail_to.move(160, 140)
        self.send_mail_to.resize(200, 30)

        self.show()

    def on_radio_button_toggled(self):
        pass

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
    def on_click(self):
        empty_fields = []
        if not self.prod_host.text():
            empty_fields.append('prod.host')
        if not self.prod_user.text():
            empty_fields.append('prod.user')
        if not self.prod_password.text():
            empty_fields.append('prod.password')
        if not self.prod_db.text():
            empty_fields.append('prod.db')
        if not self.test_host.text():
            empty_fields.append('test.host')
        if not self.test_user.text():
            empty_fields.append('test.user')
        if not self.test_password.text():
            empty_fields.append('test.password')
        if not self.test_db.text():
            empty_fields.append('test.db')
        if empty_fields:
            if len(empty_fields) == 1:
                QMessageBox.question(self, 'Error', "Please, set this parameter:\n\n" + "\n".join(empty_fields), QMessageBox.Ok,QMessageBox.Ok)
            else:
                QMessageBox.question(self, 'Error', "Please, set this parameters:\n\n" + "\n".join(empty_fields),QMessageBox.Ok, QMessageBox.Ok)
        print('Comparing started!')


        prod_host = self.prod_host.text()
        prod_user = self.prod_user.text()
        prod_password = self.prod_password.text()
        prod_db = self.prod_password.text()
        test_host = self.test_host.text()
        test_user = self.test_user.text()
        test_password = self.test_password.text()
        test_db = self.test_db.text()
        # TODO: add attempt of connection to both db. If fail - stop program and alert
        if self.cb_enableSchemaChecking.checkState() == 2:
            check_schema = True
        else:
            check_schema = False
        if self.cb_failWithFirstError.checkState() == 2:
            fail_with_first_error = True
        else:
            fail_with_first_error = False

        # TODO: add function, which prepares data to transfering it to "backend"
        prod_dict = {
            'host': prod_host,
            'user': prod_user,
            'password': prod_password,
            'db': prod_db
        }
        test_dict = {
            'host': test_host,
            'user': test_user,
            'password': test_password,
            'db': test_db
        }
        connection_dict = {
            'prod': prod_dict,
            'test': test_dict
        }
        properties = {
            'check_schema': check_schema,
            'fail_with_first_error': fail_with_first_error
        }
        backend.main(connection_dict, properties)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = Example()
    sys.exit(app.exec_())