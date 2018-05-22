from PyQt5.QtGui import QStandardItemModel, QStandardItem
from PyQt5.QtWidgets import QListView

# TODO: refactor. Make window with StandardItemModelList and OK/Cancel buttons


class SkipTablesView(QListView):
    def __init__(self, tables):
        super().__init__()
        model = QStandardItemModel()

        for table in tables:
            item = QStandardItem(table)
            item.setCheckState(0)
            item.setCheckable(True)
            model.appendRow(item)

        view = QListView()
        view.setModel(model)

        view.show()
