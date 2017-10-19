#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import os
import platform
import pymysql

from py_scripts.dbComparator.comparatorWithUI import Backend
import py_scripts.helpers.dbHelper as dbHelper
from py_scripts.helpers.logging_helper import Logger

from PyQt5.QtCore import Qt, pyqtSlot
from PyQt5.QtWidgets import QApplication, QLabel, QGridLayout, QWidget, QLineEdit, QCheckBox, QPushButton, QMessageBox
from PyQt5.QtWidgets import QFileDialog, QRadioButton, QComboBox, QAction, qApp, QMainWindow
from PyQt5.QtGui import QIcon

# TODO: add useful redacting of skip table field
# TODO: instead of QLineEdit for "skip" params you should use button with modal window? - minor
# TODO: add QSplitters - minor
# TODO: check that all parameters correctly transferred from UI to back
# TODO: add different class to work with properties

if "Win" in platform.system():
    OS = "Windows"
else:
    OS = "Linux"
if "Linux" in OS:
    propertyFile = os.getcwd() + "/resources/properties/sqlComparator.properties"
else:
    propertyFile = os.getcwd() + "\\resources\\properties\\sqlComparator.properties"


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

        advanced_label = QLabel('Advanced settings', self)
        logging_level_label = QLabel('Logging level', self)
        amount_checking_records_label = QLabel('Amount of record chunk', self)
        comparing_step_label = QLabel('Comparing step', self)
        depth_report_check_label = QLabel('Days in past', self)
        schema_columns_label = QLabel('Schema columns', self)
        retry_attempts_label = QLabel('Retry attempts', self)
        path_to_logs_label = QLabel('Path to logs', self)

        # Splitters

        # right = QFrame(self)
        # right.setFrameShape(QFrame.StyledPanel)
        # bottom = QFrame(self)
        # bottom.setFrameShape(QFrame.StyledPanel)
        # upped = QFrame(self)
        # upped.setFrameShape(QFrame.StyledPanel)
        #
        # splitter1 = QSplitter(Qt.Vertical)
        # splitter1.addWidget(right)
        # splitter2 = QSplitter(Qt.Horizontal)
        # splitter2.addWidget(bottom)
        # splitter2.addWidget(upped)

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
        self.skip_tables.setText('databasechangelog,download,migrationhistory,mntapplog,reportinfo,synchistory,' +
                                 'syncstage,synctrace,synctracelink,syncpersistentjob,forecaststatistics,' +
                                 'migrationhistory')
        self.skip_columns = QLineEdit(self)
        self.skip_columns.setText('archived,addonFields,hourOfDayS,dayOfWeekS,impCost,id')

        self.amount_checking_records = QLineEdit(self)
        self.amount_checking_records.setText('100000')
        self.comparing_step = QLineEdit(self)
        self.comparing_step.setText('10000')
        self.depth_report_check = QLineEdit(self)
        self.depth_report_check.setText('7')
        self.schema_columns = QLineEdit(self)
        # TODO: add possibility for useful redacting of schema columns parameter
        self.schema_columns.setText('TABLE_CATALOG,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,' +
                                    'IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,' +
                                    'NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,' +
                                    'COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,COLUMN_COMMENT,GENERATION_EXPRESSION')
        self.retry_attempts = QLineEdit(self)
        self.retry_attempts.setText('5')
        self.path_to_logs = QLineEdit(self)
        if OS == 'Windows':
            # TODO: add defining disc
            if not os.path.exists('C:\\DbComparator\\'):
                os.mkdir('C:\\DbComparator\\')
            self.path_to_logs.setText('C:\\DbComparator\\DbComparator.log')
        elif OS == 'Linux':
            log_path = os.path.expanduser('~') + '/DbComparator/'
            if not os.path.exists(log_path):
                os.mkdir(log_path)
            self.path_to_logs.setText(log_path + 'DbComparator.log')

        # Combobox

        self.logging_level = QComboBox(self)
        self.logging_level.addItems(['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'])

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
        btn_set_configuration = QPushButton('       Compare!       ', self)
        btn_set_configuration.clicked.connect(self.on_click)
        # btn_load_sql_params = QPushButton('Load sql params', self)
        # btn_load_sql_params.clicked.connect(self.showDialog)
        btn_clear_all = QPushButton('Clear all', self)
        btn_clear_all.clicked.connect(self.clear_all)

        # Radiobuttons

        self.day_summary_mode = QRadioButton('Day summary')
        self.day_summary_mode.setChecked(True)
        # self.day_summary_mode.toggled.connect(self.day_summary_toggled)
        self.section_summary_mode = QRadioButton('Section summary mode')
        self.section_summary_mode.setChecked(False)
        # self.section_summary_mode.toggled.connect(self.section_summary_toggled)
        self.detailed_mode = QRadioButton('Detailed mode')
        self.detailed_mode.setChecked(False)
        # self.detailed_mode.toggled.connect(self.detailed_summary_toggled)

        # self.only_entities = QRadioButton('Only entities')
        # self.only_entities.setChecked(False)
        # self.only_entities.toggled.connect(self.only_entities_toggled)
        # self.only_reports = QRadioButton('Only reports')
        # self.only_reports.setChecked(False)
        # self.only_reports.toggled.connect(self.only_reports_toggled)
        # self.both = QRadioButton('Detailed mode')
        # self.both.setChecked(True)
        # self.both.toggled.connect(self.both_toggled)


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
        btn_check_prod.setToolTip('Check connection to prod-server')
        btn_check_test.setToolTip('Check connection to test-server')

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
        grid.addWidget(btn_set_configuration, 11, 5)
        # grid.addWidget(btn_load_sql_params, 5, 0)
        grid.addWidget(checking_mode_label, 6, 3)
        grid.addWidget(self.day_summary_mode, 7, 3)
        grid.addWidget(self.section_summary_mode, 8, 3)
        grid.addWidget(self.detailed_mode, 9, 3)
        grid.addWidget(btn_clear_all, 5, 1)
        grid.addWidget(advanced_label, 0, 4)
        grid.addWidget(logging_level_label, 1, 4)
        grid.addWidget(self.logging_level, 1, 5)
        grid.addWidget(amount_checking_records_label, 2, 4)
        grid.addWidget(self.amount_checking_records, 2, 5)
        grid.addWidget(comparing_step_label, 3, 4)
        grid.addWidget(self.comparing_step, 3, 5)
        grid.addWidget(depth_report_check_label, 4, 4)
        grid.addWidget(self.depth_report_check, 4, 5)
        grid.addWidget(schema_columns_label, 5, 4)
        grid.addWidget(self.schema_columns, 5, 5)
        grid.addWidget(retry_attempts_label, 6, 4)
        grid.addWidget(self.retry_attempts, 6, 5)
        grid.addWidget(path_to_logs_label, 7, 4)
        grid.addWidget(self.path_to_logs, 7, 5)
        # grid.addWidget(self.only_entities, 8, 5)
        # grid.addWidget(self.only_reports, 9, 5)
        # grid.addWidget(self.both, 10, 5)

        self.setGeometry(0, 0, 900, 600)
        self.setWindowTitle('dbComparator')
        self.setWindowIcon(QIcon('./resources/slowpoke.png'))
        self.show()

    # def only_entities_toggled(self):
    #     pass
    #
    # def only_reports_toggled(self):
    #     pass
    #
    # def both_toggled(self):
    #     pass
    #
    # def day_summary_toggled(self):
    #     pass
    #
    # def section_summary_toggled(self):
    #     pass
    #
    # def detailed_summary_toggled(self):
    #     pass

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

    def show_dialog(self):
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
            # TODO: add loading other values from file

    def save_configuration(self):
        text = []
        if self.prod_host.text() != '':
            text.append('prod.host = {}'.format(self.prod_host.text()))
        if self.prod_user.text() != '':
            text.append('prod.user = {}'.format(self.prod_user.text()))
        if self.prod_password.text() != '':
            text.append('prod.password = {}'.format(self.prod_password.text()))
        if self.prod_db.text() != '':
            text.append('prod.dbt = {}'.format(self.prod_db.text()))
        if self.test_host.text() != '':
            text.append('test.host = {}'.format(self.test_host.text()))
        if self.test_user.text() != '':
            text.append('test_user = {}'.format(self.test_user.text()))
        if self.test_password.text() != '':
            text.append('test_password = {}'.format(self.test_password.text()))
        if self.test_db.text() != '':
            text.append('test_db = {}'.format(self.test_db.text()))
        if self.send_mail_to.text() != '':
            text.append('send_mail_to = {}'.format(self.send_mail_to.text()))
        if self.skip_tables != '':
            text.append('skip_tables = {}'.format(self.skip_tables.text()))
        if self.skip_columns != '':
            text.append('skip_columns = {}'.format(self.skip_columns.text()))
        if self.amount_checking_records != '' and self.amount_checking_records != '100000':
            text.append('amount_checking_records = {}'.format(self.amount_checking_records.text()))
        if self.comparing_step != '' and self.comparing_step != '10000':
            text.append('comparing_step = {}'.format(self.comparing_step.text()))
        if self.depth_report_check != '' and self.depth_report_check != '7':
            text.append('depth_report_check = {}'.format(self.depth_report_check.text()))
        if self.schema_columns != '' and self.schema_columns != ('TABLE_CATALOG,TABLE_NAME,COLUMN_NAME,' +
                                                                 'ORDINAL_POSITION,COLUMN_DEFAULT,' +
                                                                 'IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,' +
                                                                 'CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,' +
                                                                 'NUMERIC_SCALE,DATETIME_PRECISION,' +
                                                                 'CHARACTER_SET_NAME,COLLATION_NAME,COLUMN_TYPE,' +
                                                                 'COLUMN_KEY,EXTRA,COLUMN_COMMENT,' +
                                                                 'GENERATION_EXPRESSION'):
            text.append('schema_columns = {}'.format(self.schema_columns.text()))
        if self.retry_attempts != '' and self.retry_attempts != '5':
            text.append('retry_attempts = {}'.format(self.retry_attempts.text()))
        if self.path_to_logs != '':
            text.append('path_to_logs = {}'.format(self.path_to_logs.text()))
        fileName, _ = QFileDialog.getSaveFileName(self, "QFileDialog.getSaveFileName()",  "",
                                                  "All Files (*);;Text Files (*.txt)")
        if fileName:
            print(fileName)
            with open(fileName, 'w') as file:
                file.write('\n'.join(text))

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
        prod_host_value = self.prod_host.text()
        prod_user_value = self.prod_user.text()
        prod_password_value = self.prod_password.text()
        prod_db_value = self.prod_db.text()
        prod_dict = {
            'host': prod_host_value,
            'user': prod_user_value,
            'password': prod_password_value,
            'db': prod_db_value
        }
        try:
            dbHelper.DbConnector(prod_dict, Logger(self.logging_level.currentText())).get_tables()
            print('Prod OK!')
            QMessageBox.information(self, 'Information',
                                    "Successfully connected to\n {}/{}".format(prod_dict.get('host'),
                                                                               prod_dict.get('db')),
                                    QMessageBox.Ok, QMessageBox.Ok)
        except pymysql.OperationalError as err:
            QMessageBox.warning(self, 'Warning',
                                "Connection to {}/{} failed\n\n{}".format(prod_dict.get('host'),
                                                                          prod_dict.get('db'),
                                                                          err.args[1]),
                                QMessageBox.Ok, QMessageBox.Ok)
        except pymysql.InternalError as err:
            QMessageBox.warning(self, 'Warning', "Connection to {}/{} failed\n\n{}".format(prod_dict.get('host'),
                                                                                           prod_dict.get('db'),
                                                                                           err.args[1]),
                                QMessageBox.Ok, QMessageBox.Ok)

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
        test_host_value = self.test_host.text()
        test_user_value = self.test_user.text()
        test_password_value = self.test_password.text()
        test_db_value = self.test_db.text()
        test_dict = {
            'host': test_host_value,
            'user': test_user_value,
            'password': test_password_value,
            'db': test_db_value
        }
        try:
            dbHelper.DbConnector(test_dict, Logger(self.logging_level.currentText())).get_tables()
            print('Test OK!')
            QMessageBox.information(self, 'Information',
                                    "Successfully connected to\n {}/{}".format(test_dict.get('host'),
                                                                               test_dict.get('db')),
                                    QMessageBox.Ok, QMessageBox.Ok)
        except pymysql.OperationalError as err:
            QMessageBox.warning(self, 'Warning',
                                "Connection to {}/{} failed\n\n{}".format(test_dict.get('host'),
                                                                          test_dict.get('db'),
                                                                          err.args[1]),
                                QMessageBox.Ok, QMessageBox.Ok)
        except pymysql.InternalError as err:
            QMessageBox.warning(self, 'Warning',
                                "Connection to {}/{} failed\n\n{}".format(test_dict.get('host'),
                                                                          test_dict.get('db'),
                                                                          err.args[1]),
                                QMessageBox.Ok, QMessageBox.Ok)

    def get_sql_params(self):
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
                QMessageBox.question(self, 'Error', "Please, set this parameter:\n\n" +
                                     "\n".join(empty_fields), QMessageBox.Ok,QMessageBox.Ok)
                return False
            else:
                QMessageBox.question(self, 'Error',
                                     "Please, set this parameters:\n\n" +
                                     "\n".join(empty_fields),QMessageBox.Ok, QMessageBox.Ok)
                return False
        else:
            Logger(self.logging_level.currentText()).info('Comparing started!')
            prod_host = self.prod_host.text()
            prod_user = self.prod_user.text()
            prod_password = self.prod_password.text()
            prod_db = self.prod_db.text()
            test_host = self.test_host.text()
            test_user = self.test_user.text()
            test_password = self.test_password.text()
            test_db = self.test_db.text()
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
            connection_sql_parameters = {
                'prod': prod_dict,
                'test': test_dict
            }
            return connection_sql_parameters

    def get_properties(self):
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

        # if self.only_entities.isChecked():
        #     check_type = 'only entities'
        # elif self.only_reports.isChecked():
        #     check_type = 'only reports'
        # else:
        #     check_type = 'both'

        path_to_logs = self.path_to_logs.text()
        if path_to_logs == '':
            path_to_logs = None

        properties_dict = {
            'check_schema': check_schema,
            'fail_with_first_error': fail_with_first_error,
            'send_mail_to': self.send_mail_to.text(),
            'mode': mode,
            'skip_tables': self.skip_tables.text(),
            'skip_columns': self.skip_columns.text(),
            # 'check_type': check_type,
            'logger': Logger(self.logging_level.currentText(), path_to_logs),
            'amount_checking_records': self.amount_checking_records.text(),
            'comparing_step': self.comparing_step.text(),
            'depth_report_check': self.depth_report_check.text(),
            'schema_columns': self.schema_columns.text(),
            'retry_attempts': self.retry_attempts.text(),
            'os': OS
        }
        return properties_dict

    @pyqtSlot()
    def on_click(self):
        connection_dict = self.get_sql_params()
        properties = self.get_properties()
        if connection_dict and properties:
            Backend(connection_dict, properties).run_comparing()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.ex = Example()
        self.setCentralWidget(self.ex)

        self.setGeometry(0, 0, 900, 600)
        self.setWindowTitle('dbComparator')
        self.setWindowIcon(QIcon('./resources/slowpoke.png'))
        self.show()

        # Menu

        open_action = QAction(QIcon('open.png'), '&Open', self)
        open_action.setShortcut('Ctrl+O')
        open_action.setStatusTip('Open custom file with cmp_properties')
        open_action.triggered.connect(self.ex.show_dialog)

        save_action = QAction(QIcon('save.png'), '&Save', self)
        save_action.setShortcut('Ctrl+S')
        save_action.setStatusTip('Save current configuration to file')
        save_action.triggered.connect(self.ex.save_configuration)

        exit_action = QAction(QIcon('exit.png'), '&Exit', self)
        exit_action.setShortcut('Ctrl+Q')
        exit_action.setStatusTip('Exit application')
        exit_action.triggered.connect(qApp.quit)

        menubar = self.menuBar()
        file_menu = menubar.addMenu('&File')
        file_menu.addAction(open_action)
        file_menu.addAction(save_action)
        file_menu.addAction(exit_action)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = MainWindow()
    sys.exit(app.exec_())
