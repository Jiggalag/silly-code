import os

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QDialog, QGridLayout, QPushButton, QLabel, QLineEdit, QComboBox


# TODO: implement throwing values from advanced_settings window to main window

class AdvancedSettingsItem(QDialog):
    def __init__(self, operational_system, default_values):
        super().__init__()
        self.operational_system = operational_system
        self.default_values = default_values
        grid = QGridLayout()
        grid.setSpacing(10)
        self.setLayout(grid)

        btn_ok = QPushButton('OK', self)
        btn_ok.clicked.connect(self.ok_pressed)
        btn_cancel = QPushButton('Cancel', self)
        btn_cancel.clicked.connect(self.cancel_pressed)
        btn_reset = QPushButton('Default settings', self)
        btn_reset.clicked.connect(self.reset)

        logging_level_label = QLabel('Logging level', self)
        comparing_step_label = QLabel('Comparing step', self)
        depth_report_check_label = QLabel('Days in past', self)
        schema_columns_label = QLabel('Schema columns', self)
        retry_attempts_label = QLabel('Retry attempts', self)
        path_to_logs_label = QLabel('Path to logs', self)
        table_timeout_label = QLabel('Timeout for single table, min', self)
        strings_amount_label = QLabel('Amount of stored uniq strings', self)

        self.le_comparing_step = QLineEdit(self)
        self.le_depth_report_check = QLineEdit(self)
        self.le_schema_columns = QLineEdit(self)
        # TODO: add possibility for useful redacting of schema columns parameter
        self.le_retry_attempts = QLineEdit(self)
        self.le_path_to_logs = QLineEdit(self)
        self.le_table_timeout = QLineEdit(self)
        self.le_strings_amount = QLineEdit(self)

        # combobox

        self.cb_logging_level = QComboBox(self)
        self.cb_logging_level.addItems(['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'])
        index = self.cb_logging_level.findText('DEBUG', Qt.MatchFixedString)
        # TODO: clarify this
        if index >= 0:
            self.cb_logging_level.setCurrentIndex(index)

        # setting tooltips

        logging_level_label.setToolTip('Messages with this label and higher will be writed to logs')
        self.cb_logging_level.setToolTip('Messages with this label and higher will be writed to logs')
        comparing_step_label.setToolTip(('Max amount of records which should be requested in single sql-query\n' +
                                         'Do not touch this value if you not shure!'))
        self.le_comparing_step.setToolTip(self.le_comparing_step.text())
        depth_report_check_label.setToolTip('Amount of days in past, which should be compared in case of report tables')
        self.le_depth_report_check.setToolTip(self.le_depth_report_check.text())
        schema_columns_label.setToolTip(('List of columns, which should be compared during schema comparing\n' +
                                         'Do not touch this value if you not shure!'))
        self.le_schema_columns.setToolTip(self.le_schema_columns.text().replace(',', ',\n'))
        retry_attempts_label.setToolTip('Amount of attempts for reconnecting to dbs in case of connection lost error')
        self.le_retry_attempts.setToolTip(self.le_retry_attempts.text())
        path_to_logs_label.setToolTip('Path, where log file should be created')
        self.le_path_to_logs.setToolTip(self.le_path_to_logs.text())
        table_timeout_label.setToolTip('Timeout in minutes for checking any single table')
        self.le_table_timeout.setToolTip(self.le_table_timeout.text())
        strings_amount_label.setToolTip(('Maximum amount of uniqs for single table.\n' +
                                         'When amount of uniqs exceeds this threshould, checking of this table\n' +
                                         'will be interrupted, and uniqs will be stored in file in /tmp/comparator\n' +
                                         'directory'))
        self.le_strings_amount.setToolTip(self.le_strings_amount.text())

        grid.addWidget(logging_level_label, 0, 0)
        grid.addWidget(self.cb_logging_level, 0, 1)
        grid.addWidget(comparing_step_label, 1, 0)
        grid.addWidget(self.le_comparing_step, 1, 1)
        grid.addWidget(depth_report_check_label, 2, 0)
        grid.addWidget(self.le_depth_report_check, 2, 1)
        grid.addWidget(schema_columns_label, 3, 0)
        grid.addWidget(self.le_schema_columns, 3, 1)
        grid.addWidget(retry_attempts_label, 4, 0)
        grid.addWidget(self.le_retry_attempts, 4, 1)
        grid.addWidget(path_to_logs_label, 5, 0)
        grid.addWidget(self.le_path_to_logs, 5, 1)
        grid.addWidget(table_timeout_label, 6, 0)
        grid.addWidget(self.le_table_timeout, 6, 1)
        grid.addWidget(strings_amount_label, 7, 0)
        grid.addWidget(self.le_strings_amount, 7, 1)
        grid.addWidget(btn_ok, 8, 0)
        grid.addWidget(btn_cancel, 8, 1)
        grid.addWidget(btn_reset, 9, 0)
        self.set_default(self.default_values)

    def ok_pressed(self):
        self.logging_level = self.cb_logging_level.currentText()
        self.comparing_step = self.le_comparing_step.text()
        self.depth_report_check = self.le_depth_report_check.text()
        self.schema_columns = self.le_schema_columns.text()
        self.retry_attempts = self.le_retry_attempts.text()
        self.path_to_logs = self.le_path_to_logs.text()
        self.table_timeout = self.le_table_timeout.text()
        self.strings_amount = self.le_strings_amount.text()
        self.close()

    def cancel_pressed(self):
        self.close()

    def reset(self):
        self.set_default(self.default_values)

    def set_default(self, defaults):
        self.cb_logging_level.setCurrentIndex(4)
        self.comparing_step = defaults.get('comparing_step')
        self.le_comparing_step.setText(str(self.comparing_step))
        self.le_comparing_step.setCursorPosition(0)
        self.depth_report_check = defaults.get('depth_report_check')
        self.le_depth_report_check.setText(str(self.depth_report_check))
        self.le_depth_report_check.setCursorPosition(0)
        self.schema_columns = defaults.get('schema_columns')
        self.le_schema_columns.setText(self.schema_columns)
        self.le_schema_columns.setCursorPosition(0)
        self.retry_attempts = defaults.get('retry_attempts')
        self.le_retry_attempts.setText(str(self.retry_attempts))
        self.le_retry_attempts.setCursorPosition(0)
        self.set_path_to_logs(self.operational_system)
        self.table_timeout = defaults.get('table_timeout')
        self.le_table_timeout.setText(str(self.table_timeout))
        self.le_table_timeout.setCursorPosition(0)
        self.strings_amount = defaults.get('strings_amount')
        self.le_strings_amount.setText(str(self.strings_amount))
        self.le_strings_amount.setCursorPosition(0)

    def set_path_to_logs(self, operating_system):
        if operating_system == 'Windows':
            # TODO: add defining disc
            if not os.path.exists('C:\\DbComparator\\'):
                os.mkdir('C:\\DbComparator\\')
            self.le_path_to_logs.setText('C:\\DbComparator\\DbComparator.log')
        elif operating_system == 'Linux':
            log_path = os.path.expanduser('~') + '/DbComparator/'
            if not os.path.exists(log_path):
                os.mkdir(log_path)
            self.le_path_to_logs.setText(log_path + 'DbComparator.log')
        self.le_path_to_logs.setCursorPosition(0)
