#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import os
import platform
import pymysql

from py_scripts.dbComparator.comparatorWithUI import Backend
from py_scripts.helpers import dbcmp_sql_helper
from py_scripts.helpers.logging_helper import Logger

import PyQt5
from PyQt5.QtCore import Qt, pyqtSlot
from PyQt5.QtWidgets import QApplication, QLabel, QGridLayout, QWidget, QLineEdit, QCheckBox, QPushButton, QMessageBox
from PyQt5.QtWidgets import QFileDialog, QRadioButton, QComboBox, QAction, qApp, QMainWindow
from PyQt5.QtGui import QIcon

# TODO: add useful redacting of skip table field
# TODO: instead of QLineEdit for "skip" params you should use button with modal window? - minor
# TODO: add QSplitters - minor

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
        super(Example, self).__init__()
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
        only_tables_label = QLabel('Only tables', self)
        excluded_tables_label = QLabel('Skip tables', self)
        hide_columns_label = QLabel('Skip columns', self)

        advanced_label = QLabel('Advanced settings', self)
        logging_level_label = QLabel('Logging level', self)
        comparing_step_label = QLabel('Comparing step', self)
        depth_report_check_label = QLabel('Days in past', self)
        schema_columns_label = QLabel('Schema columns', self)
        retry_attempts_label = QLabel('Retry attempts', self)
        path_to_logs_label = QLabel('Path to logs', self)
        table_timeout_label = QLabel('Timeout for single table, min', self)
        strings_amount_label = QLabel('Amount of stored uniq strings', self)

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
        self.excluded_tables = QLineEdit(self)
        self.only_tables = QLineEdit(self)
        self.hide_columns = QLineEdit(self)
        self.comparing_step = QLineEdit(self)
        self.depth_report_check = QLineEdit(self)
        self.schema_columns = QLineEdit(self)
        # TODO: add possibility for useful redacting of schema columns parameter
        self.retry_attempts = QLineEdit(self)
        self.path_to_logs = QLineEdit(self)
        self.table_timeout = QLineEdit(self)
        self.strings_amount = QLineEdit(self)

        # Combobox

        self.logging_level = QComboBox(self)
        self.logging_level.addItems(['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'])
        index = self.logging_level.findText('DEBUG', Qt.MatchFixedString)
        if index >= 0:
            self.logging_level.setCurrentIndex(index)

        # Checkboxes

        self.cb_enable_schema_checking = QCheckBox('Compare schema', self)
        self.cb_enable_schema_checking.toggle()
        self.cb_fail_with_first_error = QCheckBox('Only first error', self)
        self.cb_fail_with_first_error.toggle()
        self.cb_only_reports_checking = QCheckBox('Only reports', self)
        self.cb_only_reports_checking.toggle()

        # Buttons

        btn_check_prod = QPushButton('Check prod connection', self)
        btn_check_prod.clicked.connect(self.check_prod)
        btn_check_test = QPushButton('Check test connection', self)
        btn_check_test.clicked.connect(self.check_test)
        btn_set_configuration = QPushButton('       Compare!       ', self)
        btn_set_configuration.setShortcut('Ctrl+F')
        btn_set_configuration.clicked.connect(self.on_click)
        btn_clear_all = QPushButton('Clear all', self)
        btn_clear_all.clicked.connect(self.clear_all)

        # Radiobuttons

        self.day_summary_mode = QRadioButton('Day summary')
        self.day_summary_mode.setChecked(True)
        self.section_summary_mode = QRadioButton('Section summary')
        self.section_summary_mode.setChecked(False)
        self.detailed_mode = QRadioButton('Detailed')
        self.detailed_mode.setChecked(False)

        self.set_default_values()

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
        btn_check_test.setToolTip('Reset all fields to default values')
        self.cb_enable_schema_checking.setToolTip('If you set this option, program will compare also schemas of dbs')
        self.cb_fail_with_first_error.setToolTip('If you set this option, comparing will be finished after first error')
        send_mail_to_label.setToolTip('Add one or list of e-mails for receiving results of comparing')
        self.send_mail_to.setToolTip('Add one or list of e-mails for receiving results of comparing')
        only_tables_label.setToolTip('Set comma-separated list of tables, which should be compared')
        self.only_tables.setToolTip('Set comma-separated list of tables, which should be compared')
        excluded_tables_label.setToolTip(self.excluded_tables.text().replace(',', ',\n'))
        self.excluded_tables.setToolTip(self.excluded_tables.text().replace(',', ',\n'))
        hide_columns_label.setToolTip(self.hide_columns.text().replace(',', ',\n'))
        self.hide_columns.setToolTip(self.hide_columns.text().replace(',', ',\n'))
        btn_set_configuration.setToolTip('Start comparing of dbs')
        btn_check_prod.setToolTip('Check connection to prod-server')
        btn_check_test.setToolTip('Check connection to test-server')
        checking_mode_label.setToolTip('Select type of checking')
        self.day_summary_mode.setToolTip('Compare sums of impressions for each date')
        self.section_summary_mode.setToolTip('Compare sums of impressions for each date and each section')
        self.detailed_mode.setToolTip('Compare all records from table for setted period')
        advanced_label.setToolTip('Advanced settings to customize your checking')
        logging_level_label.setToolTip('Messages with this label and higher will be writed to logs')
        self.logging_level.setToolTip('Messages with this label and higher will be writed to logs')
        comparing_step_label.setToolTip(('Max amount of records which should be requested in single sql-query\n' +
                                         'Do not touch this value if you not shure!'))
        self.comparing_step.setToolTip(('Max amount of records which should be requested in single sql-query\n' +
                                        'Do not touch this value if you not shure!'))
        depth_report_check_label.setToolTip('Amount of days in past, which should be compared in case of report tables')
        self.depth_report_check.setToolTip('Amount of days in past, which should be compared in case of report tables')
        schema_columns_label.setToolTip(('List of columns, which should be compared during schema comparing\n' +
                                         'Do not touch this value if you not shure!'))
        self.schema_columns.setToolTip(('List of columns, which should be compared during schema comparing\n' +
                                        'Do not touch this value if you not shure!'))
        retry_attempts_label.setToolTip('Amount of attempts for reconnecting to dbs in case of connection lost error')
        self.retry_attempts.setToolTip('Amount of attempts for reconnecting to dbs in case of connection lost error')
        path_to_logs_label.setToolTip('Path, where log file should be created')
        self.path_to_logs.setToolTip('Path, where log file should be created')
        table_timeout_label.setToolTip('Timeout in minutes for checking any single table')
        self.table_timeout.setToolTip('Timeout in minutes for checking any single table')
        strings_amount_label.setToolTip(('Maximum amount of uniqs for single table.\n' +
                                         'When amount of uniqs exceeds this threshould, checking of this table\n' +
                                         'will be interrupted, and uniqs will be stored in file in /tmp/comparator\n' +
                                         'directory'))
        self.strings_amount.setToolTip(('Maximum amount of uniqs for single table.\n' +
                                        'When amount of uniqs exceeds this threshould, checking of this table\n' +
                                        'will be interrupted, and uniqs will be stored in file in /tmp/comparator\n' +
                                        'directory'))

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
        grid.addWidget(only_tables_label, 7, 0)
        grid.addWidget(self.only_tables, 7, 1)
        grid.addWidget(excluded_tables_label, 8, 0)
        grid.addWidget(self.excluded_tables, 8, 1)
        grid.addWidget(hide_columns_label, 9, 0)
        grid.addWidget(self.hide_columns, 9, 1)
        grid.addWidget(self.cb_enable_schema_checking, 10, 0)
        grid.addWidget(self.cb_fail_with_first_error, 11, 0)
        grid.addWidget(self.cb_only_reports_checking, 10, 1)
        grid.addWidget(btn_set_configuration, 11, 5)
        grid.addWidget(checking_mode_label, 6, 3)
        grid.addWidget(self.day_summary_mode, 7, 3)
        grid.addWidget(self.section_summary_mode, 8, 3)
        grid.addWidget(self.detailed_mode, 9, 3)
        grid.addWidget(btn_clear_all, 5, 1)
        grid.addWidget(advanced_label, 0, 4)
        grid.addWidget(logging_level_label, 1, 4)
        grid.addWidget(self.logging_level, 1, 5)
        grid.addWidget(comparing_step_label, 2, 4)
        grid.addWidget(self.comparing_step, 2, 5)
        grid.addWidget(depth_report_check_label, 3, 4)
        grid.addWidget(self.depth_report_check, 3, 5)
        grid.addWidget(schema_columns_label, 4, 4)
        grid.addWidget(self.schema_columns, 4, 5)
        grid.addWidget(retry_attempts_label, 5, 4)
        grid.addWidget(self.retry_attempts, 5, 5)
        grid.addWidget(path_to_logs_label, 6, 4)
        grid.addWidget(self.path_to_logs, 6, 5)
        grid.addWidget(table_timeout_label, 7, 4)
        grid.addWidget(self.table_timeout, 7, 5)
        grid.addWidget(strings_amount_label, 8, 4)
        grid.addWidget(self.strings_amount, 8, 5)

        self.setWindowTitle('dbComparator')
        self.setWindowIcon(QIcon('./resources/slowpoke.png'))
        self.show()

    def clear_all(self):
        self.prod_host.clear()
        self.prod_user.clear()
        self.prod_password.clear()
        self.prod_db.clear()
        self.test_host.clear()
        self.test_user.clear()
        self.test_password.clear()
        self.test_db.clear()
        self.send_mail_to.clear()
        self.only_tables.clear()
        self.set_default_values()

    def set_default_values(self):
        self.excluded_tables.setText('databasechangelog,download,migrationhistory,mntapplog,reportinfo,synchistory,' +
                                     'syncstage,synctrace,synctracelink,syncpersistentjob,forecaststatistics,' +
                                     'migrationhistory')
        self.hide_columns.setText('archived,addonFields,hourOfDayS,dayOfWeekS,impCost,id')
        self.comparing_step.setText('10000')
        self.depth_report_check.setText('7')
        self.schema_columns.setText('TABLE_CATALOG,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,' +
                                    'IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,' +
                                    'NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,' +
                                    'COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,COLUMN_COMMENT,GENERATION_EXPRESSION')
        self.retry_attempts.setText('5')
        self.set_path_to_logs(OS)
        self.table_timeout.setText('5')
        self.strings_amount.setText('1000')
        self.cb_enable_schema_checking.setChecked(True)
        self.cb_fail_with_first_error.setChecked(True)
        self.day_summary_mode.setChecked(True)
        self.section_summary_mode.setChecked(False)
        self.detailed_mode.setChecked(False)

    def set_path_to_logs(self, OS):
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

    def show_dialog(self):
        current_dir = '{}/resources/properties/'.format(os.getcwd())
        fname = QFileDialog.getOpenFileName(PyQt5.QtWidgets.QFileDialog(), 'Open file', current_dir)[0]
        with open(fname, 'r') as file:
            data = file.read()
            for record in data.split('\n'):
                string = record.replace(' ', '')
                if 'prod.host' in string:
                    host = string[string.find('=') + 1:]
                    self.prod_host.setText(host)
                elif 'prod.user' in string:
                    user = string[string.find('=') + 1:]
                    self.prod_user.setText(user)
                elif 'prod.password' in string:
                    password = string[string.find('=') + 1:]
                    self.prod_password.setText(password)
                elif 'prod.db' in string:
                    db = string[string.find('=') + 1:]
                    self.prod_db.setText(db)
                elif 'test.host' in string:
                    host = string[string.find('=') + 1:]
                    self.test_host.setText(host)
                elif 'test.user' in string:
                    user = string[string.find('=') + 1:]
                    self.test_user.setText(user)
                elif 'test.password' in string:
                    password = string[string.find('=') + 1:]
                    self.test_password.setText(password)
                elif 'test.db' in string:
                    db = string[string.find('=') + 1:]
                    self.test_db.setText(db)
                elif 'only_tables' in string:
                    only_tables = string[string.find('=') + 1:]
                    self.only_tables.setText(only_tables)
                elif 'excluded_tables' in string:
                    excluded_tables = string[string.find('=') + 1:]
                    self.excluded_tables.setText(excluded_tables)
                elif 'comparing_step' in string:
                    comparing_step = string[string.find('=') + 1:]
                    self.comparing_step.setText(comparing_step)
                elif 'depth_report_check' in string:
                    depth_report_check = string[string.find('=') + 1:]
                    self.depth_report_check.setText(depth_report_check)
                elif 'schema_columns' in string:
                    schema_columns = string[string.find('=') + 1:]
                    self.schema_columns.setText(schema_columns)
                elif 'retry_attempts' in string:
                    retry_attempts = string[string.find('=') + 1:]
                    self.retry_attempts.setText(retry_attempts)
                elif 'path_to_logs' in string:
                    path_to_logs = string[string.find('=') + 1:]
                    self.path_to_logs.setText(path_to_logs)
                elif 'send_mail_to' in string:
                    send_mail_to = string[string.find('=') + 1:]
                    self.send_mail_to.setText(send_mail_to)
                elif 'compare_schema' in string:
                    compare_schema = string[string.find('=') + 1:]
                    if compare_schema == 'True':
                        if self.cb_enable_schema_checking.isChecked():
                            pass
                        else:
                            self.cb_enable_schema_checking.setChecked(True)
                    else:
                        if self.cb_enable_schema_checking.isChecked():
                            self.cb_enable_schema_checking.setChecked(False)
                        else:
                            pass
                elif 'fail_with_first_error' in string:
                    only_first_error = string[string.find('=') + 1:]
                    if only_first_error == 'True':
                        if self.cb_fail_with_first_error.isChecked():
                            pass
                        else:
                            self.cb_fail_with_first_error.setChecked(True)
                    else:
                        if self.cb_fail_with_first_error.isChecked():
                            self.cb_fail_with_first_error.setChecked(False)
                        else:
                            pass
                elif 'only_reports' in string:
                    only_reports = string[string.find('=') + 1:]
                    if only_reports == 'True':
                        if self.cb_only_reports_checking.isChecked():
                            pass
                        else:
                            self.cb_only_reports_checking.setChecked(True)
                    else:
                        if self.cb_only_reports_checking.isChecked():
                            self.cb_only_reports_checking.setChecked(False)
                        else:
                            pass
                elif 'logging_level' in string:
                    logging_level = string[string.find('=') + 1:]
                    index = self.logging_level.findText(logging_level, Qt.MatchFixedString)
                    if index >= 0:
                        self.logging_level.setCurrentIndex(index)
                elif 'table_timeout' in string:
                    table_timeout = string[string.find('=') + 1:]
                    self.table_timeout.setText(table_timeout)
                elif 'mode' in string:
                    mode = string[string.find('=') + 1:]
                    if mode == 'day-sum':
                        self.day_summary_mode.setChecked(True)
                    elif mode == 'section-sum':
                        self.section_summary_mode.setChecked(True)
                    else:
                        self.detailed_mode.setChecked(True)

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
            text.append('test.user = {}'.format(self.test_user.text()))
        if self.test_password.text() != '':
            text.append('test.password = {}'.format(self.test_password.text()))
        if self.test_db.text() != '':
            text.append('test.db = {}'.format(self.test_db.text()))
        if self.send_mail_to.text() != '':
            text.append('send_mail_to = {}'.format(self.send_mail_to.text()))
        if self.only_tables != '':
            text.append('only_tables = {}'.format(self.only_tables.text()))
        if self.excluded_tables != '':
            text.append('excluded_tables = {}'.format(self.excluded_tables.text()))
        if self.hide_columns != '':
            text.append('hide_columns = {}'.format(self.hide_columns.text()))
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
        if self.send_mail_to != '':
            text.append('send_mail_to = {}'.format(self.send_mail_to.text()))
        if self.path_to_logs != '':
            text.append('path_to_logs = {}'.format(self.path_to_logs.text()))
        if self.table_timeout != '':
            text.append('table_timeout = {}'.format(self.table_timeout.text()))
        if self.cb_enable_schema_checking.isChecked():
            text.append('compare_schema = True')
        if not self.cb_enable_schema_checking.isChecked():
            text.append('compare_schema = False')
        if self.cb_fail_with_first_error.isChecked():
            text.append('fail_with_first_error = True')
        if not self.cb_fail_with_first_error.isChecked():
            text.append('fail_with_first_error = False')
        if self.day_summary_mode.isChecked():
            text.append('mode = day-sum')
        elif self.section_summary_mode.isChecked():
            text.append('mode = section-sum')
        elif self.detailed_mode.isChecked():
            text.append('mode = detailed')
        text.append('logging_level = {}'.format(self.logging_level.currentText()))
        file_name, _ = QFileDialog.getSaveFileName(PyQt5.QtWidgets.QFileDialog(), "QFileDialog.getSaveFileName()",  "",
                                                   "All Files (*);;Text Files (*.txt)")
        if file_name:
            print(file_name)
            with open(file_name, 'w') as file:
                file.write('\n'.join(text))

    @staticmethod
    def exit():
        sys.exit(0)

    def check_prod(self, disable_mboxes):
        logger = Logger(self.logging_level.currentText())
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
                QMessageBox.question(PyQt5.QtWidgets.QMessageBox(), 'Error',
                                     "Please, set this parameter:\n\n" + "\n".join(empty_fields),
                                     QMessageBox.Ok, QMessageBox.Ok)
                return False
            else:
                QMessageBox.question(PyQt5.QtWidgets.QMessageBox(), 'Error',
                                     "Please, set this parameters:\n\n" + "\n".join(empty_fields),
                                     QMessageBox.Ok, QMessageBox.Ok)
                return False
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
            dbcmp_sql_helper.DbCmpSqlHelper(prod_dict, logger).get_tables()
            logger.info('Connection to db {} established successfully!'.format(self.prod_db.text()))
            if not disable_mboxes:
                QMessageBox.information(PyQt5.QtWidgets.QMessageBox(), 'Information',
                                        "Successfully connected to\n {}/".format(prod_dict.get('host')) +
                                        "{}".format(prod_dict.get('db')),
                                        QMessageBox.Ok, QMessageBox.Ok)
            return True
        except pymysql.OperationalError as err:
            logger.warn("Connection to {}/{} failed\n\n{}".format(prod_dict.get('host'), prod_dict.get('db'),
                                                                  err.args[1]))
            QMessageBox.warning(PyQt5.QtWidgets.QMessageBox(), 'Warning',
                                "Connection to {}/{} ".format(prod_dict.get('host'), prod_dict.get('db')) +
                                "failed\n\n{}".format(err.args[1]),
                                QMessageBox.Ok, QMessageBox.Ok)
            return False
        except pymysql.InternalError as err:
            logger.warn("Connection to {}/{} failed\n\n{}".format(prod_dict.get('host'), prod_dict.get('db'),
                                                                  err.args[1]))
            QMessageBox.warning(PyQt5.QtWidgets.QMessageBox(), 'Warning',
                                "Connection to {}/{} ".format(prod_dict.get('host'), prod_dict.get('db')) +
                                "failed\n\n{}".format(err.args[1]),
                                QMessageBox.Ok, QMessageBox.Ok)
            return False

    def check_test(self, disable_mboxes):
        logger = Logger(self.logging_level.currentText())
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
            if not disable_mboxes:
                QMessageBox.question(PyQt5.QtWidgets.QMessageBox(), 'Error',
                                     "Please, set this parameter:\n\n" + "\n".join(empty_fields),
                                     QMessageBox.Ok, QMessageBox.Ok)
                return False
            else:
                QMessageBox.question(PyQt5.QtWidgets.QMessageBox(), 'Error',
                                     "Please, set this parameters:\n\n" + "\n".join(empty_fields),
                                     QMessageBox.Ok, QMessageBox.Ok)
                return False
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
            dbcmp_sql_helper.DbCmpSqlHelper(test_dict, logger).get_tables()
            logger.info('Connection to db {} established successfully!'.format(self.test_db.text()))
            if not disable_mboxes:
                QMessageBox.information(PyQt5.QtWidgets.QMessageBox(), 'Information',
                                        "Successfully connected to\n {}/{}".format(test_dict.get('host'),
                                                                                   test_dict.get('db')),
                                        QMessageBox.Ok, QMessageBox.Ok)
            return True
        except pymysql.OperationalError as err:
            logger.warn("Connection to {}/{} ".format(test_dict.get('host'), test_dict.get('db')) +
                        "failed\n\n{}".format(err.args[1]))
            QMessageBox.warning(PyQt5.QtWidgets.QMessageBox(), 'Warning',
                                "Connection to {}/{} failed\n\n{}".format(test_dict.get('host'),
                                                                          test_dict.get('db'),
                                                                          err.args[1]),
                                QMessageBox.Ok, QMessageBox.Ok)
            return False
        except pymysql.InternalError as err:
            logger.warn("Connection to {}/{} ".format(test_dict.get('host'), test_dict.get('db')) +
                        "failed\n\n{}".format(err.args[1]))
            QMessageBox.warning(PyQt5.QtWidgets.QMessageBox(), 'Warning',
                                "Connection to {}/{} ".format(test_dict.get('host'), test_dict.get('db')) +
                                "failed\n\n{}".format(err.args[1]),
                                QMessageBox.Ok, QMessageBox.Ok)
            return False

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
                QMessageBox.question(PyQt5.QtWidgets.QMessageBox(), 'Error', "Please, set this parameter:\n\n" +
                                     "\n".join(empty_fields), QMessageBox.Ok, QMessageBox.Ok)
                return False
            else:
                QMessageBox.question(PyQt5.QtWidgets.QMessageBox(), 'Error', "Please, set this parameters:\n\n" +
                                     "\n".join(empty_fields), QMessageBox.Ok, QMessageBox.Ok)
                return False
        else:
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

        if self.cb_only_reports_checking.checkState() == 2:
            only_reports = True
        else:
            only_reports = False

        if self.day_summary_mode.isChecked():
            mode = 'day-sum'
        elif self.section_summary_mode.isChecked():
            mode = 'section-sum'
        else:
            mode = 'detailed'

        path_to_logs = self.path_to_logs.text()
        if path_to_logs == '':
            path_to_logs = None

        properties_dict = {
            'check_schema': check_schema,
            'fail_with_first_error': fail_with_first_error,
            'send_mail_to': self.send_mail_to.text(),
            'mode': mode,
            'excluded_tables': self.excluded_tables.text(),
            'hide_columns': self.hide_columns.text(),
            'strings_amount': self.strings_amount.text(),
            # 'check_type': check_type,
            'logger': Logger(self.logging_level.currentText(), path_to_logs),
            'comparing_step': self.comparing_step.text(),
            'depth_report_check': self.depth_report_check.text(),
            'schema_columns': self.schema_columns.text(),
            'retry_attempts': self.retry_attempts.text(),
            'only_tables': self.only_tables.text(),
            'only_reports': only_reports,
            # TODO: add try/catch
            'table_timeout': int(self.table_timeout.text()),
            'os': OS
        }
        return properties_dict

    @pyqtSlot()
    def on_click(self):
        connection_dict = self.get_sql_params()
        properties = self.get_properties()
        if connection_dict and properties:
            if self.check_prod('quiet') and self.check_test('quiet'):
                Logger(self.logging_level.currentText()).info('Comparing started!')
                Backend(connection_dict, properties).run_comparing()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.ex = Example()
        self.setCentralWidget(self.ex)

        self.setGeometry(300, 300, 900, 600)
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
