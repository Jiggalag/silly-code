#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import platform
import sys

import PyQt5
import pymysql
from PyQt5.QtCore import pyqtSlot
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication, QLabel, QGridLayout, QWidget, QLineEdit, QCheckBox, QPushButton, QMessageBox
from PyQt5.QtWidgets import QFileDialog, QRadioButton, QAction, qApp, QMainWindow

from py_scripts.dbComparator.advanced_settings import AdvancedSettingsItem
from py_scripts.dbComparator.clickable_lineedit import ClickableLineEdit
from py_scripts.dbComparator.comparatorWithUI import Backend
from py_scripts.dbComparator.skip_tables_view import ClickableItemsView
from py_scripts.helpers import dbcmp_sql_helper
from py_scripts.helpers.logging_helper import Logger

# TODO: add QSplitters - minor

if "Win" in platform.system():
    operating_system = "Windows"
else:
    operating_system = "Linux"
if "Linux" in operating_system:
    propertyFile = os.getcwd() + "/resources/properties/sqlComparator.properties"
else:
    propertyFile = os.getcwd() + "\\resources\\properties\\sqlComparator.properties"


class MainUI(QWidget):
    def __init__(self, status_bar):
        super().__init__()
        self.statusBar = status_bar
        self.statusBar.messageChanged.connect(self.calculate_table_list)
        grid = QGridLayout()
        grid.setSpacing(10)
        self.setLayout(grid)

        self.tables = list()
        self.prod_tables = list()
        self.test_tables = list()

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

        self.le_prod_host = QLineEdit(self)
        self.le_prod_host.textChanged.connect(self.check_prod_connection)
        self.le_prod_user = QLineEdit(self)
        self.le_prod_user.textChanged.connect(self.check_prod_connection)
        self.le_prod_password = QLineEdit(self)
        self.le_prod_password.textChanged.connect(self.check_prod_connection)
        self.le_prod_db = QLineEdit(self)
        self.le_prod_db.textChanged.connect(self.check_prod_connection)
        self.le_test_host = QLineEdit(self)
        self.le_test_host.textChanged.connect(self.check_test_connection)
        self.le_test_user = QLineEdit(self)
        self.le_test_user.textChanged.connect(self.check_test_connection)
        self.le_test_password = QLineEdit(self)
        self.le_test_password.textChanged.connect(self.check_test_connection)
        self.le_test_db = QLineEdit(self)
        self.le_test_db.textChanged.connect(self.check_test_connection)
        self.le_send_mail_to = QLineEdit(self)
        self.le_excluded_tables = ClickableLineEdit(self)
        self.le_excluded_tables.clicked.connect(self.set_excluded_tables)
        self.le_only_tables = ClickableLineEdit(self)
        self.le_only_tables.clicked.connect(self.set_included_tables)
        self.le_skip_columns = QLineEdit(self)

        # Checkboxes

        self.cb_enable_schema_checking = QCheckBox('Compare schema', self)
        self.cb_enable_schema_checking.toggle()
        self.cb_fail_with_first_error = QCheckBox('Only first error', self)
        self.cb_fail_with_first_error.toggle()
        self.cb_reports = QCheckBox('Reports', self)
        self.cb_reports.toggle()
        self.cb_entities = QCheckBox('Entities and others', self)
        self.cb_entities.toggle()

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
        btn_advanced = QPushButton('Advanced', self)
        btn_advanced.clicked.connect(self.advanced)

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
        self.le_prod_host.setToolTip(self.le_prod_host.text())
        prod_user_label.setToolTip('Input user for connection to prod-db.\nExample: itest')
        self.le_prod_user.setToolTip(self.le_prod_user.text())
        prod_password_label.setToolTip('Input password for user from prod.sql-user field')
        self.le_prod_password.setToolTip(self.le_prod_password.text())
        prod_db_label.setToolTip('Input prod-db name.\nExample: irving')
        self.le_prod_db.setToolTip(self.le_prod_db.text())
        test_host_label.setToolTip('Input host, where test-db located.\nExample: samaradb03.maxifier.com')
        self.le_test_host.setToolTip(self.le_test_host.text())
        test_user_label.setToolTip('Input user for connection to test-db.\nExample: itest')
        self.le_test_user.setToolTip(self.le_test_user.text())
        test_password_label.setToolTip('Input password for user from test.sql-user field')
        self.le_test_password.setToolTip(self.le_test_password.text())
        test_db_label.setToolTip('Input test-db name.\nExample: irving')
        self.le_test_db.setToolTip(self.le_test_db.text())
        btn_check_test.setToolTip('Reset all fields to default values')
        self.cb_enable_schema_checking.setToolTip('If you set this option, program will compare also schemas of dbs')
        self.cb_fail_with_first_error.setToolTip('If you set this option, comparing will be finished after first error')
        send_mail_to_label.setToolTip('Add one or list of e-mails for receiving results of comparing')
        self.le_send_mail_to.setToolTip(self.le_send_mail_to.text().replace(',', ',\n'))
        only_tables_label.setToolTip('Set comma-separated list of tables, which should be compared')
        self.le_only_tables.setToolTip(self.le_only_tables.text().replace(',', ',\n'))
        excluded_tables_label.setToolTip('Set tables, which should not be checked')
        self.le_excluded_tables.setToolTip(self.le_excluded_tables.text().replace(',', ',\n'))
        hide_columns_label.setToolTip('Set columns, which should not be compared during checking')
        self.le_skip_columns.setToolTip(self.le_skip_columns.text().replace(',', ',\n'))
        btn_set_configuration.setToolTip('Start comparing of dbs')
        btn_check_prod.setToolTip('Check connection to prod-server')
        btn_check_test.setToolTip('Check connection to test-server')
        checking_mode_label.setToolTip('Select type of checking')
        self.day_summary_mode.setToolTip('Compare sums of impressions for each date')
        self.section_summary_mode.setToolTip('Compare sums of impressions for each date and each section')
        self.detailed_mode.setToolTip('Compare all records from table for setted period')

        grid.addWidget(prod_host_label, 0, 0)
        grid.addWidget(self.le_prod_host, 0, 1)
        grid.addWidget(prod_user_label, 1, 0)
        grid.addWidget(self.le_prod_user, 1, 1)
        grid.addWidget(prod_password_label, 2, 0)
        grid.addWidget(self.le_prod_password, 2, 1)
        grid.addWidget(prod_db_label, 3, 0)
        grid.addWidget(self.le_prod_db, 3, 1)
        grid.addWidget(test_host_label, 0, 2)
        grid.addWidget(self.le_test_host, 0, 3)
        grid.addWidget(test_user_label, 1, 2)
        grid.addWidget(self.le_test_user, 1, 3)
        grid.addWidget(test_password_label, 2, 2)
        grid.addWidget(self.le_test_password, 2, 3)
        grid.addWidget(test_db_label, 3, 2)
        grid.addWidget(self.le_test_db, 3, 3)
        grid.addWidget(btn_check_prod, 4, 1)
        grid.addWidget(btn_check_test, 4, 3)
        grid.addWidget(send_mail_to_label, 6, 0)
        grid.addWidget(self.le_send_mail_to, 6, 1)
        grid.addWidget(only_tables_label, 7, 0)
        grid.addWidget(self.le_only_tables, 7, 1)
        grid.addWidget(excluded_tables_label, 8, 0)
        grid.addWidget(self.le_excluded_tables, 8, 1)
        grid.addWidget(hide_columns_label, 9, 0)
        grid.addWidget(self.le_skip_columns, 9, 1)
        grid.addWidget(self.cb_enable_schema_checking, 10, 0)
        grid.addWidget(self.cb_fail_with_first_error, 11, 0)
        grid.addWidget(self.cb_reports, 10, 1)
        grid.addWidget(self.cb_entities, 11, 1)
        grid.addWidget(checking_mode_label, 6, 3)
        grid.addWidget(self.day_summary_mode, 7, 3)
        grid.addWidget(self.section_summary_mode, 8, 3)
        grid.addWidget(self.detailed_mode, 9, 3)
        grid.addWidget(btn_clear_all, 5, 1)
        grid.addWidget(btn_advanced, 10, 3)
        grid.addWidget(btn_set_configuration, 11, 3)

        self.setWindowTitle('dbComparator')
        self.setWindowIcon(QIcon('./resources/slowpoke.png'))
        self.show()

    def clear_all(self):
        self.le_prod_host.clear()
        self.le_prod_user.clear()
        self.le_prod_password.clear()
        self.le_prod_db.clear()
        self.le_test_host.clear()
        self.le_test_user.clear()
        self.le_test_password.clear()
        self.le_test_db.clear()
        self.le_send_mail_to.clear()
        self.le_only_tables.clear()
        self.set_default_values()

    def advanced(self):
        defaults = {
            'logging_level': self.logging_level,
            'comparing_step': self.comparing_step,
            'depth_report_check': self.depth_report_check,
            'schema_columns': self.schema_columns,
            'retry_attempts': self.retry_attempts,
            'path_to_logs': self.path_to_logs,
            'table_timeout': self.table_timeout,
            'strings_amount': self.strings_amount
        }
        self.adv = AdvancedSettingsItem(operating_system, defaults)
        self.adv.exec_()
        self.logging_level = self.adv.logging_level
        self.comparing_step = self.adv.comparing_step
        self.depth_report_check = self.adv.depth_report_check
        self.schema_columns = self.adv.schema_columns
        self.retry_attempts = self.adv.retry_attempts
        self.path_to_logs = self.adv.path_to_logs
        self.table_timeout = self.adv.table_timeout
        self.strings_amount = self.adv.strings_amount

    def calculate_table_list(self):
        if self.statusBar.currentMessage() == 'Prod connected, test connected':
            self.tables = list(set(self.prod_tables) & set(self.test_tables))
            self.tables.sort()

    def set_default_values(self):
        self.le_excluded_tables.setText(
            'databasechangelog,download,migrationhistory,mntapplog,reportinfo,synchistory,' +
                                     'syncstage,synctrace,synctracelink,syncpersistentjob,forecaststatistics,' +
                                     'migrationhistory')
        self.le_excluded_tables.setCursorPosition(0)
        self.le_skip_columns.setText('archived,addonFields,hourOfDayS,dayOfWeekS,impCost,id')
        self.le_skip_columns.setCursorPosition(0)
        self.comparing_step = 10000
        self.depth_report_check = 7
        self.schema_columns = ('TABLE_CATALOG,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,' +
                               'IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,' +
                               'NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,' +
                               'COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,COLUMN_COMMENT,GENERATION_EXPRESSION')
        self.retry_attempts = 5
        if operating_system == 'Windows':
            # TODO: add defining disc
            if not os.path.exists('C:\\DbComparator\\'):
                os.mkdir('C:\\DbComparator\\')
            self.path_to_logs = 'C:\\DbComparator\\DbComparator.log'
        elif operating_system == 'Linux':
            log_path = os.path.expanduser('~') + '/DbComparator/'
            if not os.path.exists(log_path):
                os.mkdir(log_path)
            self.path_to_logs = log_path + 'DbComparator.log'
        self.table_timeout = 5
        self.strings_amount = 1000
        self.cb_enable_schema_checking.setChecked(True)
        self.cb_fail_with_first_error.setChecked(True)
        self.day_summary_mode.setChecked(True)
        self.section_summary_mode.setChecked(False)
        self.detailed_mode.setChecked(False)
        self.logging_level = 'DEBUG'

    def set_excluded_tables(self):
        if self.statusBar.currentMessage() == 'Prod connected, test connected':
            tables_to_skip = self.le_excluded_tables.text().split(',')
            skip_tables_view = ClickableItemsView(self.tables, tables_to_skip)
            skip_tables_view.exec_()
            self.le_excluded_tables.setText(','.join(skip_tables_view.selected_items))
            self.le_excluded_tables.setToolTip(self.le_excluded_tables.text().replace(',', ',\n'))

    def set_included_tables(self):
        if self.statusBar.currentMessage() == 'Prod connected, test connected':
            tables_to_include = self.le_only_tables.text().split(',')
            only_tables_view = ClickableItemsView(self.tables, tables_to_include)
            only_tables_view.exec_()
            self.le_only_tables.setText(','.join(only_tables_view.selected_items))
            self.le_only_tables.setToolTip(self.le_only_tables.text().replace(',', ',\n'))

    def check_prod_connection(self):
        states = self.statusBar.currentMessage().split(', ')
        prod_state = states[0]
        test_state = states[1]
        if all([self.le_prod_host.text(), self.le_prod_user.text(), self.le_prod_password.text(),
                self.le_prod_db.text()]):
            prod_dict = {
                'host': self.le_prod_host.text(),
                'user': self.le_prod_user.text(),
                'password': self.le_prod_password.text(),
                'db': self.le_prod_db.text()
            }
            logger = Logger(self.logging_level)
            prod_state = 'Prod disconnected'
            for attempt in range(1, 3):
                kwargs = {'read_timeout': attempt}
                try:
                    self.prod_tables = dbcmp_sql_helper.DbCmpSqlHelper(prod_dict, logger, **kwargs).get_tables()
                    if self.prod_tables:
                        prod_state = 'Prod connected'
                        break
                except pymysql.InternalError as e:
                    logger.error('Exception is {}'.format(e.args[1]))
        self.statusBar.showMessage('{}, {}'.format(prod_state, test_state))

    def check_test_connection(self):
        states = self.statusBar.currentMessage().split(', ')
        prod_state = states[0]
        test_state = states[1]
        if all([self.le_test_host.text(), self.le_test_user.text(), self.le_test_password.text(),
                self.le_test_db.text()]):
            test_dict = {
                'host': self.le_test_host.text(),
                'user': self.le_test_user.text(),
                'password': self.le_test_password.text(),
                'db': self.le_test_db.text()
            }
            logger = Logger(self.logging_level)
            test_state = 'test disconnected'
            for attempt in range(1, 3):
                kwargs = {'read_timeout': attempt}
                try:
                    self.test_tables = dbcmp_sql_helper.DbCmpSqlHelper(test_dict, logger, **kwargs).get_tables()
                    if self.test_tables:
                        test_state = 'test connected'
                except pymysql.InternalError as e:
                    logger.error('Exception is {}'.format(e.args[1]))
        self.statusBar.showMessage('{}, {}'.format(prod_state, test_state))

    def show_dialog(self):
        current_dir = '{}/resources/properties/'.format(os.getcwd())
        fname = QFileDialog.getOpenFileName(PyQt5.QtWidgets.QFileDialog(), 'Open file', current_dir)[0]
        with open(fname, 'r') as file:
            data = file.read()
            for record in data.split('\n'):
                string = record.replace(' ', '')
                if 'prod.host' in string:
                    host = string[string.find('=') + 1:]
                    self.le_prod_host.setText(host)
                    self.le_prod_host.setCursorPosition(0)
                elif 'prod.user' in string:
                    user = string[string.find('=') + 1:]
                    self.le_prod_user.setText(user)
                    self.le_prod_user.setCursorPosition(0)
                elif 'prod.password' in string:
                    password = string[string.find('=') + 1:]
                    self.le_prod_password.setText(password)
                    self.le_prod_password.setCursorPosition(0)
                elif 'prod.db' in string:
                    db = string[string.find('=') + 1:]
                    self.le_prod_db.setText(db)
                    self.le_prod_db.setCursorPosition(0)
                elif 'test.host' in string:
                    host = string[string.find('=') + 1:]
                    self.le_test_host.setText(host)
                    self.le_test_host.setCursorPosition(0)
                elif 'test.user' in string:
                    user = string[string.find('=') + 1:]
                    self.le_test_user.setText(user)
                    self.le_test_user.setCursorPosition(0)
                elif 'test.password' in string:
                    password = string[string.find('=') + 1:]
                    self.le_test_password.setText(password)
                    self.le_test_password.setCursorPosition(0)
                elif 'test.db' in string:
                    db = string[string.find('=') + 1:]
                    self.le_test_db.setText(db)
                    self.le_test_db.setCursorPosition(0)
                elif 'only_tables' in string:
                    only_tables = string[string.find('=') + 1:]
                    self.le_only_tables.setText(only_tables)
                    self.le_only_tables.setCursorPosition(0)
                elif 'skip_tables' in string:
                    excluded_tables = string[string.find('=') + 1:]
                    self.le_excluded_tables.setText(excluded_tables)
                    self.le_excluded_tables.setCursorPosition(0)
                elif 'comparing_step' in string:
                    self.comparing_step = string[string.find('=') + 1:]
                elif 'depth_report_check' in string:
                    self.depth_report_check = string[string.find('=') + 1:]
                elif 'schema_columns' in string:
                    self.schema_columns = string[string.find('=') + 1:]
                elif 'retry_attempts' in string:
                    self.retry_attempts = string[string.find('=') + 1:]
                elif 'path_to_logs' in string:
                    self.path_to_logs = string[string.find('=') + 1:]
                elif 'send_mail_to' in string:
                    send_mail_to = string[string.find('=') + 1:]
                    self.le_send_mail_to.setText(send_mail_to)
                    self.le_send_mail_to.setCursorPosition(0)
                elif 'skip_columns' in string:
                    skip_columns = string[string.find('=') + 1:]
                    self.le_skip_columns.setText(skip_columns)
                    self.le_skip_columns.setCursorPosition(0)
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
                elif 'reports' in string:
                    reports = string[string.find('=') + 1:]
                    if reports == 'True':
                        if self.cb_reports.isChecked():
                            pass
                        else:
                            self.cb_reports.setChecked(True)
                    else:
                        if self.cb_reports.isChecked():
                            self.cb_reports.setChecked(False)
                        else:
                            pass
                elif 'entities' in string:
                    entities = string[string.find('=') + 1:]
                    if entities == 'True':
                        if self.cb_entities.isChecked():
                            pass
                        else:
                            self.cb_entities.setChecked(True)
                    else:
                        if self.cb_entities.isChecked():
                            self.cb_entities.setChecked(False)
                        else:
                            pass
                elif 'logging_level' in string:
                    self.logging_level = string[string.find('=') + 1:]
                elif 'table_timeout' in string:
                    self.table_timeout = string[string.find('=') + 1:]
                elif 'mode' in string:
                    mode = string[string.find('=') + 1:]
                    if mode == 'day-sum':
                        self.day_summary_mode.setChecked(True)
                    elif mode == 'section-sum':
                        self.section_summary_mode.setChecked(True)
                    else:
                        self.detailed_mode.setChecked(True)

        # Set tooltips

        self.le_prod_host.setToolTip(self.le_prod_host.text())
        self.le_prod_user.setToolTip(self.le_prod_user.text())
        self.le_prod_password.setToolTip(self.le_prod_password.text())
        self.le_prod_db.setToolTip(self.le_prod_db.text())
        self.le_test_host.setToolTip(self.le_test_host.text())
        self.le_test_user.setToolTip(self.le_test_user.text())
        self.le_test_password.setToolTip(self.le_test_password.text())
        self.le_test_db.setToolTip(self.le_test_db.text())
        self.le_only_tables.setToolTip(self.le_only_tables.text().replace(',', ',\n'))
        self.le_excluded_tables.setToolTip(self.le_excluded_tables.text().replace(',', ',\n'))
        self.le_skip_columns.setToolTip(self.le_skip_columns.text().replace(',', ',\n'))
        self.le_send_mail_to.setToolTip(self.le_send_mail_to.text().replace(',', ',\n'))

    def save_configuration(self):
        text = []
        non_verified = {}
        if self.le_prod_host.text() != '':
            text.append('prod.host = {}'.format(self.le_prod_host.text()))
        if self.le_prod_user.text() != '':
            text.append('prod.user = {}'.format(self.le_prod_user.text()))
        if self.le_prod_password.text() != '':
            text.append('prod.password = {}'.format(self.le_prod_password.text()))
        if self.le_prod_db.text() != '':
            text.append('prod.dbt = {}'.format(self.le_prod_db.text()))
        if self.le_test_host.text() != '':
            text.append('test.host = {}'.format(self.le_test_host.text()))
        if self.le_test_user.text() != '':
            text.append('test.user = {}'.format(self.le_test_user.text()))
        if self.le_test_password.text() != '':
            text.append('test.password = {}'.format(self.le_test_password.text()))
        if self.le_test_db.text() != '':
            text.append('test.db = {}'.format(self.le_test_db.text()))
        if self.le_send_mail_to.text() != '':
            raw_array = self.le_send_mail_to.text().split(',')
            raw_array.sort()
            text.append('send_mail_to = {}'.format(str(raw_array).strip('[]').replace("'", "").replace(' ', '')))
        if self.le_only_tables.text() != '':
            raw_array = self.le_only_tables.text().split(',')
            raw_array.sort()
            text.append('only_tables = {}'.format(str(raw_array).strip('[]').replace("'", "").replace(' ', '')))
        if self.le_excluded_tables.text() != '':
            raw_array = self.le_excluded_tables.text().split(',')
            raw_array.sort()
            text.append('skip_tables = {}'.format(str(raw_array).strip('[]').replace("'", "").replace(' ', '')))
        if self.le_skip_columns.text() != '':
            raw_array = self.le_skip_columns.text().split(',')
            raw_array.sort()
            text.append('skip_columns = {}'.format(str(raw_array).strip('[]').replace("'", "").replace(' ', '')))
        if self.comparing_step != '' and self.comparing_step != '10000':
            try:
                int(self.comparing_step)
                text.append('comparing_step = {}'.format(self.comparing_step))
            except ValueError:
                non_verified.update({'Comparing step': self.comparing_step})
        if self.depth_report_check != '' and self.depth_report_check != '7':
            try:
                int(self.depth_report_check)
                text.append('depth_report_check = {}'.format(self.depth_report_check))
            except ValueError:
                non_verified.update({'Days in past': self.depth_report_check})
        default_column_text = ('TABLE_CATALOG,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,' +
                               'IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,' +
                               'NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,' +
                               'COLUMN_TYPE,COLUMN_KEY,EXTRA,COLUMN_COMMENT,GENERATION_EXPRESSION')

        if self.schema_columns != '' and self.schema_columns != default_column_text:
            raw_array = self.schema_columns.split(',')
            raw_array.sort()
            text.append('schema_columns = {}'.format(str(raw_array).strip('[]').replace("'", "").replace(' ', '')))
        if self.retry_attempts != '' and self.retry_attempts != '5':
            try:
                int(self.retry_attempts)
                text.append('retry_attempts = {}'.format(self.retry_attempts))
            except ValueError:
                non_verified.update({'Retry attempts': self.retry_attempts})
        if self.path_to_logs != '':
            text.append('path_to_logs = {}'.format(self.path_to_logs))
        if self.table_timeout != '':
            try:
                int(self.table_timeout)
                text.append('table_timeout = {}'.format(self.table_timeout))
            except ValueError:
                non_verified.update({'Timeout for single table': self.table_timeout})
        if self.strings_amount != '':
            try:
                int(self.strings_amount)
                text.append('string_amount = {}'.format(self.strings_amount))
            except ValueError:
                non_verified.update({'Amount of stored uniq strings': self.strings_amount})
        if non_verified:
            text = ''
            for item in non_verified.keys():
                text = '{}\n{}: {}'.format(text, item, non_verified.get(item))
            QMessageBox.warning(PyQt5.QtWidgets.QMessageBox(), 'Error',
                                ("Incorrect value(s):\n{}\n\n".format(text) +
                                 "Please, input a number!"),
                                QMessageBox.Ok, QMessageBox.Ok)
            return False
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
        text.append('logging_level = {}'.format(self.logging_level))
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
        logger = Logger(self.logging_level)
        empty_fields = []
        if not self.le_prod_host.text():
            empty_fields.append('prod.host')
        if not self.le_prod_user.text():
            empty_fields.append('prod.user')
        if not self.le_prod_password.text():
            empty_fields.append('prod.password')
        if not self.le_prod_db.text():
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
        prod_host_value = self.le_prod_host.text()
        prod_user_value = self.le_prod_user.text()
        prod_password_value = self.le_prod_password.text()
        prod_db_value = self.le_prod_db.text()
        prod_dict = {
            'host': prod_host_value,
            'user': prod_user_value,
            'password': prod_password_value,
            'db': prod_db_value
        }
        try:
            dbcmp_sql_helper.DbCmpSqlHelper(prod_dict, logger).get_tables()
            logger.info('Connection to db {} established successfully!'.format(self.le_prod_db.text()))
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
        logger = Logger(self.logging_level)
        empty_fields = []
        if not self.le_test_host.text():
            empty_fields.append('test.host')
        if not self.le_test_user.text():
            empty_fields.append('test.user')
        if not self.le_test_password.text():
            empty_fields.append('test.password')
        if not self.le_test_db.text():
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
        test_host_value = self.le_test_host.text()
        test_user_value = self.le_test_user.text()
        test_password_value = self.le_test_password.text()
        test_db_value = self.le_test_db.text()
        test_dict = {
            'host': test_host_value,
            'user': test_user_value,
            'password': test_password_value,
            'db': test_db_value
        }
        try:
            dbcmp_sql_helper.DbCmpSqlHelper(test_dict, logger).get_tables()
            logger.info('Connection to db {} established successfully!'.format(self.le_test_db.text()))
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
        if not self.le_prod_host.text():
            empty_fields.append('prod.host')
        if not self.le_prod_user.text():
            empty_fields.append('prod.user')
        if not self.le_prod_password.text():
            empty_fields.append('prod.password')
        if not self.le_prod_db.text():
            empty_fields.append('prod.db')
        if not self.le_test_host.text():
            empty_fields.append('test.host')
        if not self.le_test_user.text():
            empty_fields.append('test.user')
        if not self.le_test_password.text():
            empty_fields.append('test.password')
        if not self.le_test_db.text():
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
            prod_host = self.le_prod_host.text()
            prod_user = self.le_prod_user.text()
            prod_password = self.le_prod_password.text()
            prod_db = self.le_prod_db.text()
            test_host = self.le_test_host.text()
            test_user = self.le_test_user.text()
            test_password = self.le_test_password.text()
            test_db = self.le_test_db.text()
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

        if self.cb_reports.checkState() == 2:
            reports = True
        else:
            reports = False

        if self.cb_entities.checkState() == 2:
            entities = True
        else:
            entities = False

        if self.day_summary_mode.isChecked():
            mode = 'day-sum'
        elif self.section_summary_mode.isChecked():
            mode = 'section-sum'
        else:
            mode = 'detailed'

        path_to_logs = self.path_to_logs
        if path_to_logs == '':
            path_to_logs = None

        properties_dict = {
            'check_schema': check_schema,
            'fail_with_first_error': fail_with_first_error,
            'send_mail_to': self.le_send_mail_to.text(),
            'mode': mode,
            'skip_tables': self.le_excluded_tables.text(),
            'hide_columns': self.le_skip_columns.text(),
            'strings_amount': self.strings_amount,
            # 'check_type': check_type,
            'logger': Logger(self.logging_level, path_to_logs),
            'comparing_step': self.comparing_step,
            'depth_report_check': self.depth_report_check,
            'schema_columns': self.schema_columns,
            'retry_attempts': self.retry_attempts,
            'only_tables': self.le_only_tables.text(),
            'reports': reports,
            'entities': entities,
            # TODO: add try/catch?
            'table_timeout': int(self.table_timeout),
            'os': operating_system
        }
        return properties_dict

    @pyqtSlot()
    def on_click(self):
        connection_dict = self.get_sql_params()
        properties = self.get_properties()
        if connection_dict and properties:
            if self.check_prod('quiet') and self.check_test('quiet'):
                Logger(self.logging_level).info('Comparing started!')
                Backend(connection_dict, properties).run_comparing()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.statusBar = self.statusBar()
        self.statusBar.showMessage('Prod disconnected, test disconnected')
        self.ex = MainUI(self.statusBar)
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

        compare_action = QAction(QIcon('compare.png'), '&Compare', self)
        compare_action.setShortcut('Ctrl+F')
        compare_action.setStatusTip('Run comparing')
        compare_action.triggered.connect(self.ex.on_click)

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
        file_menu.addAction(compare_action)
        file_menu.addAction(exit_action)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = MainWindow()
    sys.exit(app.exec_())
