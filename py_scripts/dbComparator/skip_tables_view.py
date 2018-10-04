from PyQt5.QtGui import QStandardItemModel, QStandardItem
from PyQt5.QtWidgets import QListView, QDialog, QGridLayout, QPushButton


class ClickableItemsView(QDialog):
    def __init__(self, tables, selected_items):
        super().__init__()
        grid = QGridLayout()
        grid.setSpacing(10)
        self.setLayout(grid)
        self.selected_items = selected_items
        self.tables = tables

        btn_ok = QPushButton('OK', self)
        btn_ok.clicked.connect(self.ok_pressed)
        btn_cancel = QPushButton('Cancel', self)
        btn_cancel.clicked.connect(self.cancel_pressed)

        self.view = self.init_items()

        grid.addWidget(self.view, 0, 0)
        grid.addWidget(btn_ok, 1, 1)
        grid.addWidget(btn_cancel, 1, 2)
        self.setModal(True)
        self.show()

    def init_items(self):
        self.model = QStandardItemModel()

        for table in self.tables:
            item = QStandardItem(table)
            item.setCheckState(0)
            if item.text() in self.selected_items:
                item.setCheckState(2)
            item.setCheckable(True)
            self.model.appendRow(item)

        view = QListView()
        view.setModel(self.model)
        return view

    def ok_pressed(self):
        amount = self.model.rowCount()
        checked_tables = list()
        for idx in range(0, amount - 1):
            item = self.model.item(idx, 0)
            if item.checkState() == 2:
                checked_tables.append(item.text())
        self.selected_items = checked_tables
        self.init_items()
        self.close()

    def cancel_pressed(self):
        self.close()
