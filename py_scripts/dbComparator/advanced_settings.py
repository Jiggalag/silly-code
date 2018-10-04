import os

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QDialog, QGridLayout, QPushButton, QLabel, QLineEdit, QComboBox


# TODO: implement throwing values from advanced_settings window to main window

class AdvancedSettingsItem(QDialog):
    def __init__(self, operational_system):
        super().__init__()
        self.operational_system = operational_system
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

        self.comparing_step = QLineEdit(self)
        self.depth_report_check = QLineEdit(self)
        self.schema_columns = QLineEdit(self)
        # TODO: add possibility for useful redacting of schema columns parameter
        self.retry_attempts = QLineEdit(self)
        self.path_to_logs = QLineEdit(self)
        self.table_timeout = QLineEdit(self)
        self.strings_amount = QLineEdit(self)

        # combobox

        self.logging_level = QComboBox(self)
        self.logging_level.addItems(['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'])
        index = self.logging_level.findText('DEBUG', Qt.MatchFixedString)
        if index >= 0:
            self.logging_level.setCurrentIndex(index)

        # setting tooltips

        logging_level_label.setToolTip('Messages with this label and higher will be writed to logs')
        self.logging_level.setToolTip('Messages with this label and higher will be writed to logs')
        comparing_step_label.setToolTip(('Max amount of records which should be requested in single sql-query\n' +
                                         'Do not touch this value if you not shure!'))
        self.comparing_step.setToolTip(self.comparing_step.text())
        depth_report_check_label.setToolTip('Amount of days in past, which should be compared in case of report tables')
        self.depth_report_check.setToolTip(self.depth_report_check.text())
        schema_columns_label.setToolTip(('List of columns, which should be compared during schema comparing\n' +
                                         'Do not touch this value if you not shure!'))
        self.schema_columns.setToolTip(self.schema_columns.text().replace(',', ',\n'))
        retry_attempts_label.setToolTip('Amount of attempts for reconnecting to dbs in case of connection lost error')
        self.retry_attempts.setToolTip(self.retry_attempts.text())
        path_to_logs_label.setToolTip('Path, where log file should be created')
        self.path_to_logs.setToolTip(self.path_to_logs.text())
        table_timeout_label.setToolTip('Timeout in minutes for checking any single table')
        self.table_timeout.setToolTip(self.table_timeout.text())
        strings_amount_label.setToolTip(('Maximum amount of uniqs for single table.\n' +
                                         'When amount of uniqs exceeds this threshould, checking of this table\n' +
                                         'will be interrupted, and uniqs will be stored in file in /tmp/comparator\n' +
                                         'directory'))
        self.strings_amount.setToolTip(self.strings_amount.text())

        grid.addWidget(logging_level_label, 0, 0)
        grid.addWidget(self.logging_level, 0, 1)
        grid.addWidget(comparing_step_label, 1, 0)
        grid.addWidget(self.comparing_step, 1, 1)
        grid.addWidget(depth_report_check_label, 2, 0)
        grid.addWidget(self.depth_report_check, 2, 1)
        grid.addWidget(schema_columns_label, 3, 0)
        grid.addWidget(self.schema_columns, 3, 1)
        grid.addWidget(retry_attempts_label, 4, 0)
        grid.addWidget(self.retry_attempts, 4, 1)
        grid.addWidget(path_to_logs_label, 5, 0)
        grid.addWidget(self.path_to_logs, 5, 1)
        grid.addWidget(table_timeout_label, 6, 0)
        grid.addWidget(self.table_timeout, 6, 1)
        grid.addWidget(strings_amount_label, 7, 0)
        grid.addWidget(self.strings_amount, 7, 1)
        grid.addWidget(btn_ok, 8, 0)
        grid.addWidget(btn_cancel, 8, 1)
        grid.addWidget(btn_reset, 9, 0)
        self.set_default()

    def ok_pressed(self):
        pass

    def cancel_pressed(self):
        pass

    def reset(self):
        self.set_default()

    def set_default(self):
        self.logging_level.setCurrentIndex(4)
        self.comparing_step.setText('10000')
        self.comparing_step.setCursorPosition(0)
        self.depth_report_check.setText('7')
        self.depth_report_check.setCursorPosition(0)
        self.schema_columns.setText('TABLE_CATALOG,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,' +
                                    'IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,' +
                                    'NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,' +
                                    'COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,COLUMN_COMMENT,GENERATION_EXPRESSION')
        self.schema_columns.setCursorPosition(0)
        self.retry_attempts.setText('5')
        self.retry_attempts.setCursorPosition(0)
        self.set_path_to_logs(self.operational_system)
        self.table_timeout.setText('5')
        self.table_timeout.setCursorPosition(0)
        self.strings_amount.setText('1000')
        self.strings_amount.setCursorPosition(0)

    def set_path_to_logs(self, operational_system):
        if operational_system == 'Windows':
            # TODO: add defining disc
            if not os.path.exists('C:\\DbComparator\\'):
                os.mkdir('C:\\DbComparator\\')
            self.path_to_logs.setText('C:\\DbComparator\\DbComparator.log')
        elif operational_system == 'Linux':
            log_path = os.path.expanduser('~') + '/DbComparator/'
            if not os.path.exists(log_path):
                os.mkdir(log_path)
            self.path_to_logs.setText(log_path + 'DbComparator.log')
        self.path_to_logs.setCursorPosition(0)
