from PyQt5.QtWidgets import QGridLayout, QWidget, QLabel, QLineEdit, QRadioButton, QPushButton


class AdvancedSettings(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        grid_advanced = QGridLayout()
        grid_advanced.setSpacing(10)
        self.setLayout(grid_advanced)

        # Labels

        logging_level_label = QLabel('Logging level')
        amount_checking_records_label = QLabel('Amount of record chunk', self)
        comparing_step_label = QLabel('Comparing step', self)
        depth_report_check_label = QLabel('Days in past', self)
        schema_columns_label = QLabel('Schema columns', self)
        retry_attempts_label = QLabel('Retry attempts', self)

        # Line edits

        self.logging_level = QLineEdit(self)
        self.logging_level.setText('20')
        self.amount_checking_records = QLineEdit(self)
        self.amount_checking_records.setText('100000')
        self.comparing_step = QLineEdit(self)
        self.comparing_step.setText('10000')
        self.depth_report_check = QLineEdit(self)
        self.depth_report_check.setText('7')
        self.schema_columns = QLineEdit(self)
        # TODO: add possibility for useful redacting of schema columns parameter
        self.schema_columns.setText('TABLE_CATALOG,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,COLUMN_COMMENT,GENERATION_EXPRESSION')
        self.retry_attempts = QLineEdit(self)
        self.retry_attempts.setText('5')

# TODO: add tooltips

        # Radiobuttons

        self.only_entities = QRadioButton('Only entities')
        self.only_entities.setChecked(False)
        self.only_entities.toggled.connect(self.only_entities_toggled)
        self.only_reports = QRadioButton('Only reports')
        self.only_reports.setChecked(False)
        self.only_reports.toggled.connect(self.only_reports_toggled)
        self.both = QRadioButton('Detailed mode')
        self.both.setChecked(True)
        self.both.toggled.connect(self.both_toggled)

        # Buttons

        btn_advanced_ok = QPushButton('OK', self)
        btn_advanced_ok.clicked.connect(self.advanced_ok)

        grid_advanced.addWidget(logging_level_label, 0, 0)
        grid_advanced.addWidget(self.logging_level, 0, 1)
        grid_advanced.addWidget(amount_checking_records_label, 1, 0)
        grid_advanced.addWidget(self.amount_checking_records, 1, 1)
        grid_advanced.addWidget(comparing_step_label, 2, 0)
        grid_advanced.addWidget(self.comparing_step, 2, 1)
        grid_advanced.addWidget(depth_report_check_label, 3, 0)
        grid_advanced.addWidget(self.depth_report_check, 3, 1)
        grid_advanced.addWidget(schema_columns_label, 4, 0)
        grid_advanced.addWidget(self.schema_columns, 4, 1)
        grid_advanced.addWidget(retry_attempts_label, 5, 0)
        grid_advanced.addWidget(self.retry_attempts, 5, 1)
        grid_advanced.addWidget(self.only_entities, 0, 2)
        grid_advanced.addWidget(self.only_reports, 1, 2)
        grid_advanced.addWidget(self.both, 2, 2)
        grid_advanced.addWidget(btn_advanced_ok, 5, 2)

    def only_entities_toggled(self):
        pass

    def only_reports_toggled(self):
        pass

    def both_toggled(self):
        pass

    def advanced_ok(self):
        self.close()
