#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys

import os

import pymysql

from PyQt5.QtCore import Qt, pyqtSlot
from PyQt5.QtWidgets import QApplication, QLabel, QGridLayout, QWidget, QLineEdit, QCheckBox, QPushButton, QMessageBox, QFileDialog, QRadioButton
from PyQt5.QtGui import QIcon
import py_scripts.dbComparator.comparatorWithUI as backend
import py_scripts.helpers.dbHelper as dbHelper
# TODO: add useful redacting of skip table field
# TODO:instead of QLineEdit for "skip" params you should use button with modal window
# TODO: probably, add menu

class Example(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        grid = QGridLayout()
        grid.setSpacing(10)
        self.setLayout(grid)

        # Labels

        prod_host_label = QLabel('prod.sql-host')
        prod_user_label = QLabel('prod.sql-user', self)
        prod_password_label = QLabel('prod.sql-password', self)
        prod_db_label = QLabel('prod.sql-db', self)
        test_host_label = QLabel('test.sql-host', self)
        test_user_label = QLabel('test.sql-user', self)
        test_password_label = QLabel('test.sql-password', self)
        test_db_label = QLabel('test.sql-db', self)
        send_mail_to_label = QLabel('Send mail to', self)
        checking_mode_label = QLabel('Checking mode:', self)
        skip_tables_label = QLabel('Skip tables', self)
        skip_columns_label = QLabel('Skip columns', self)

        # Line edits

        self.prod_host = QLineEdit(self)
        self.prod_user = QLineEdit(self)
        self.prod_password = QLineEdit(self)
        self.prod_db = QLineEdit(self)
        self.test_host = QLineEdit(self)
        self.test_user = QLineEdit(self)
        self.test_password = QLineEdit(self)
        self.test_db = QLineEdit(self)
        self.send_mail_to = QLineEdit(self)
        self.skip_tables = QLineEdit(self)
        self.skip_tables.setText('databasechangelog,download,migrationhistory,mntapplog,reportinfo,synchistory,syncstage,synctrace,synctracelink,syncpersistentjob,forecaststatistics,migrationhistory')
        self.skip_columns = QLineEdit(self)
        self.skip_columns.setText('archived,addonFields,hourOfDayS,dayOfWeekS,impCost,id')

        # Checkboxes

        self.cb_enable_schema_checking = QCheckBox('Compare schema', self)
        self.cb_enable_schema_checking.setToolTip('If you set this option, program will compare also schemas of dbs')
        self.cb_enable_schema_checking.toggle()
        self.cb_enable_schema_checking.stateChanged.connect(self.enableSchemaCheckingConstruct)
        self.cb_fail_with_first_error = QCheckBox('Only first error', self)
        self.cb_fail_with_first_error.setToolTip('If you set this option, comparing will be finished after first error')
        self.cb_fail_with_first_error.toggle()
        self.cb_fail_with_first_error.stateChanged.connect(self.failWithFirstErrorConstruct)

        # Buttons

        btn_check_prod = QPushButton('Check prod connection', self)
        btn_check_prod.clicked.connect(self.check_prod)
        btn_check_test = QPushButton('Check test connection', self)
        btn_check_test.clicked.connect(self.check_test)
        btn_set_configuration = QPushButton('Compare!', self)
        btn_set_configuration.clicked.connect(self.on_click)
        btn_load_sql_params = QPushButton('Load sql params', self)
        btn_load_sql_params.clicked.connect(self.showDialog)
        btn_clear_all = QPushButton('Clear all', self)
        btn_clear_all.clicked.connect(self.clear_all)

        # Radiobuttons

        self.day_summary_mode = QRadioButton('Day summary')
        self.day_summary_mode.setChecked(True)
        self.day_summary_mode.toggled.connect(self.day_summary_toggled)
        self.section_summary_mode = QRadioButton('Section summary mode')
        self.section_summary_mode.setChecked(False)
        self.section_summary_mode.toggled.connect(self.section_summary_toggled)
        self.detailed_mode = QRadioButton('Detailed mode')
        self.detailed_mode.setChecked(False)
        self.detailed_mode.toggled.connect(self.detailed_summary_toggled)

        # Set tooltips

        prod_host_label.setToolTip('Input host, where prod-db located.\nExample: samaradb03.maxifier.com')
        self.prod_host.setToolTip('Input host, where prod-db located.\nExample: samaradb03.maxifier.com')
        prod_user_label.setToolTip('Input user for connection to prod-db.\nExample: itest')
        self.prod_user.setToolTip('Input user for connection to prod-db.\nExample: itest')
        prod_password_label.setToolTip('Input password for user from prod.sql-user field')
        self.prod_password.setToolTip('Input password for user from prod.sql-user field')
        prod_db_label.setToolTip('Input prod-db name.\nExample: irving')
        self.prod_db.setToolTip('Input prod-db name.\nExample: irving')
        test_host_label.setToolTip('Input host, where test-db located.\nExample: samaradb03.maxifier.com')
        self.test_host.setToolTip('Input host, where test-db located.\nExample: samaradb03.maxifier.com')
        test_user_label.setToolTip('Input user for connection to test-db.\nExample: itest')
        self.test_user.setToolTip('Input user for connection to test-db.\nExample: itest')
        test_password_label.setToolTip('Input password for user from test.sql-user field')
        self.test_password.setToolTip('Input password for user from test.sql-user field')
        test_db_label.setToolTip('Input test-db name.\nExample: irving')
        self.test_db.setToolTip('Input test-db name.\nExample: irving')
        send_mail_to_label.setToolTip('Add one or list of e-mails for receiving results of comparing')
        self.send_mail_to.setToolTip('Add one or list of e-mails for receiving results of comparing')
        skip_tables_label.setToolTip(self.skip_tables.text().replace(',', ',\n'))
        self.skip_tables.setToolTip(self.skip_tables.text().replace(',', ',\n'))
        skip_columns_label.setToolTip(self.skip_columns.text().replace(',', ',\n'))
        self.skip_columns.setToolTip(self.skip_columns.text().replace(',', ',\n'))
        btn_set_configuration.setToolTip('Start comparing of dbs')
        # TODO: add tooltips for all widgets

        grid.addWidget(prod_host_label, 0, 0)
        grid.addWidget(self.prod_host, 0, 1)
        grid.addWidget(prod_user_label, 1, 0)
        grid.addWidget(self.prod_user, 1, 1)
        grid.addWidget(prod_password_label, 2, 0)
        grid.addWidget(self.prod_password, 2, 1)
        grid.addWidget(prod_db_label, 3, 0)
        grid.addWidget(self.prod_db, 3, 1)
        grid.addWidget(test_host_label, 0, 2)
        grid.addWidget(self.test_host, 0, 3)
        grid.addWidget(test_user_label, 1, 2)
        grid.addWidget(self.test_user, 1, 3)
        grid.addWidget(test_password_label, 2, 2)
        grid.addWidget(self.test_password, 2, 3)
        grid.addWidget(test_db_label, 3, 2)
        grid.addWidget(self.test_db, 3, 3)
        grid.addWidget(btn_check_prod, 4, 1)
        grid.addWidget(btn_check_test, 4, 3)
        grid.addWidget(send_mail_to_label, 6, 0)
        grid.addWidget(self.send_mail_to, 6, 1)
        grid.addWidget(skip_tables_label, 7, 0)
        grid.addWidget(self.skip_tables, 7, 1)
        grid.addWidget(skip_columns_label, 8, 0)
        grid.addWidget(self.skip_columns, 8, 1)
        grid.addWidget(self.cb_enable_schema_checking, 9, 0)
        grid.addWidget(self.cb_fail_with_first_error, 10, 0)
        grid.addWidget(btn_set_configuration, 10, 3)
        grid.addWidget(btn_load_sql_params, 5, 0)
        grid.addWidget(checking_mode_label, 5, 3)
        grid.addWidget(self.day_summary_mode, 6, 3)
        grid.addWidget(self.section_summary_mode, 7, 3)
        grid.addWidget(self.detailed_mode, 8, 3)
        grid.addWidget(btn_clear_all, 5, 1)

        self.setGeometry(0, 0, 900, 600)
        self.setWindowTitle('dbComparator')
        self.setWindowIcon(QIcon('./resources/slowpoke.png'))
        self.show()

    def day_summary_toggled(self):
        pass

    def section_summary_toggled(self):
        pass

    def detailed_summary_toggled(self):
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

    def clear_all(self):
        self.prod_host.clear()
        self.prod_user.clear()
        self.prod_password.clear()
        self.prod_password.clear()
        self.test_host.clear()
        self.test_user.clear()
        self.test_password.clear()
        self.test_db.clear()
        self.send_mail_to.clear()

    def showDialog(self):
        current_dir = os.getcwd()
        fname = QFileDialog.getOpenFileName(self, 'Open file', current_dir)[0]
        with open(fname, 'r') as file:
            data = file.read()
            for record in data.split('\n'):
                string = record.replace(' ', '')
                if 'prod' in string:
                    if 'host' in string:
                        host = string[string.find('=') + 1:]
                        self.prod_host.setText(host)
                    elif 'user' in string:
                        user = string[string.find('=') + 1:]
                        self.prod_user.setText(user)
                    elif 'password' in string:
                        password = string[string.find('=') + 1:]
                        self.prod_password.setText(password)
                    elif 'db' in string:
                        db = string[string.find('=') + 1:]
                        self.prod_db.setText(db)
                elif 'test' in string:
                    if 'host' in string:
                        host = string[string.find('=') + 1:]
                        self.test_host.setText(host)
                    elif 'user' in string:
                        user = string[string.find('=') + 1:]
                        self.test_user.setText(user)
                    elif 'password' in string:
                        password = string[string.find('=') + 1:]
                        self.test_password.setText(password)
                    elif 'db' in string:
                        db = string[string.find('=') + 1:]
                        self.test_db.setText(db)

    def exit(self):
        sys.exit(0)

    def check_prod(self):
        empty_fields = []
        if not self.prod_host.text():
            empty_fields.append('prod.host')
        if not self.prod_user.text():
            empty_fields.append('prod.user')
        if not self.prod_password.text():
            empty_fields.append('prod.password')
        if not self.prod_db.text():
            empty_fields.append('prod.db')
        if empty_fields:
            if len(empty_fields) == 1:
                QMessageBox.question(self, 'Error', "Please, set this parameter:\n\n" + "\n".join(empty_fields),
                                     QMessageBox.Ok, QMessageBox.Ok)
            else:
                QMessageBox.question(self, 'Error', "Please, set this parameters:\n\n" + "\n".join(empty_fields),
                                     QMessageBox.Ok, QMessageBox.Ok)
        prod_host = self.prod_host.text()
        prod_user = self.prod_user.text()
        prod_password = self.prod_password.text()
        prod_db = self.prod_db.text()
        prod_dict = {
            'host': prod_host,
            'user': prod_user,
            'password': prod_password,
            'db': prod_db
        }
        try:
            dbHelper.DbConnector(prod_dict).get_tables()
            print('Prod OK!')
            QMessageBox.information(self, 'Information', "Successfully connected to\n {}/{}".format(prod_dict.get('host'), prod_dict.get('db')),
                                 QMessageBox.Ok, QMessageBox.Ok)
        except pymysql.OperationalError as err:
            QMessageBox.warning(self, 'Warning', "Connection to {}/{} failed\n\n{}".format(prod_dict.get('host'), prod_dict.get('db'), err.args[1]), QMessageBox.Ok, QMessageBox.Ok)
        except pymysql.InternalError as err:
            QMessageBox.warning(self, 'Warning', "Connection to {}/{} failed\n\n{}".format(prod_dict.get('host'), prod_dict.get('db'), err.args[1]), QMessageBox.Ok, QMessageBox.Ok)

    def check_test(self):
        empty_fields = []
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
                QMessageBox.question(self, 'Error', "Please, set this parameter:\n\n" + "\n".join(empty_fields),
                                     QMessageBox.Ok, QMessageBox.Ok)
            else:
                QMessageBox.question(self, 'Error', "Please, set this parameters:\n\n" + "\n".join(empty_fields),
                                     QMessageBox.Ok, QMessageBox.Ok)
        test_host = self.test_host.text()
        test_user = self.test_user.text()
        test_password = self.test_password.text()
        test_db = self.test_db.text()
        test_dict = {
            'host': test_host,
            'user': test_user,
            'password': test_password,
            'db': test_db
        }
        try:
            dbHelper.DbConnector(test_dict).get_tables()
            print('Test OK!')
            QMessageBox.information(self, 'Information', "Successfully connected to\n {}/{}".format(test_dict.get('host'), test_dict.get('db')),
                                 QMessageBox.Ok, QMessageBox.Ok)
        except pymysql.OperationalError as err:
            QMessageBox.warning(self, 'Warning', "Connection to {}/{} failed\n\n{}".format(test_dict.get('host'), test_dict.get('db'), err.args[1]), QMessageBox.Ok, QMessageBox.Ok)
        except pymysql.InternalError as err:
            QMessageBox.warning(self, 'Warning', "Connection to {}/{} failed\n\n{}".format(test_dict.get('host'), test_dict.get('db'), err.args[1]), QMessageBox.Ok, QMessageBox.Ok)

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
        else:
            print('Comparing started!')
            prod_host = self.prod_host.text()
            prod_user = self.prod_user.text()
            prod_password = self.prod_password.text()
            prod_db = self.prod_db.text()
            test_host = self.test_host.text()
            test_user = self.test_user.text()
            test_password = self.test_password.text()
            test_db = self.test_db.text()
            # TODO: add attempt of connection to both db. If fail - stop program and alert
            if self.cb_enable_schema_checking.checkState() == 2:
                check_schema = True
            else:
                check_schema = False
            if self.cb_fail_with_first_error.checkState() == 2:
                fail_with_first_error = True
            else:
                fail_with_first_error = False

            if self.day_summary_mode.isChecked():
                mode = 'day-sum'
            elif self.section_summary_mode.isChecked():
                mode = 'section-sum'
            else:
                mode = 'detailed'

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
                'fail_with_first_error': fail_with_first_error,
                'send_mail_to': self.send_mail_to.text(),
                'mode': mode
            }
            backend.runComparing(connection_dict, properties)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = Example()
    sys.exit(app.exec_())