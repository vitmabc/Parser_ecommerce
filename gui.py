from shops import piramida24 as piramida24
from shops import teplozapchast as teplozapchast
from shops import kotel_kr as kotel_kr
from shops import gazkomplekt as gazkomplekt
from shops import pivot
import os
import sys
from design import *
from PyQt6 import QtCore, QtGui, QtWidgets


class WorkerSignals(QtCore.QObject):
    stepChanged = QtCore.pyqtSignal(str)
    finished = QtCore.pyqtSignal(str)
    error = QtCore.pyqtSignal(str)


class Thread(QtCore.QRunnable):

    def __init__(self, module, name):
        super().__init__()
        self.module = module
        self.name = name
        self.mem_signal = WorkerSignals()

    def run(self):
        try:
            self.module()
            self.mem_signal.finished.emit(f'{self.name}')
        except:
            self.mem_signal.error.emit('')


class PivotTable(QtCore.QRunnable):

    def __init__(self):
        super().__init__()
        self.file = 'result.xlsx'
        self.mem_signal = WorkerSignals()

    def run(self):
        try:
            pivot.start()
            self.mem_signal.finished.emit('')
            os.startfile(self.file)
        except:
            self.mem_signal.error.emit('')


class MainProgram(QtWidgets.QMainWindow):
    def __init__(self, parent=None):
        QtWidgets.QWidget.__init__(self, parent)
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.threadpool = QtCore.QThreadPool()
        self.add_function()

    def add_function(self):
        self.ui.pushButton.clicked.connect(lambda: self.start_main())

    def start_main(self):
        self.ui.textEdit.setText(
            'Скрипт почав працювати.\nДочекайтесь закінчення, посля чого зможете відкрити\nотримані файли Excel ')
        self.ui.pushButton.setText('В процесі')
        self.ui.pushButton.setEnabled(False)
        thread1 = Thread(piramida24.main, 'Piramida24')
        thread1.daemon = True
        thread1.mem_signal.finished.connect(self.onStepChanged)
        thread1.mem_signal.error.connect(self.error)
        self.threadpool.start(thread1)
        thread2 = Thread(teplozapchast.start, 'Teplozapchast')
        thread2.daemon = True
        thread2.mem_signal.finished.connect(self.onStepChanged)
        thread2.mem_signal.error.connect(self.error)
        self.threadpool.start(thread2)

        thread3 = Thread(gazkomplekt.start, 'Gazkomplekt')
        thread3.daemon = True
        thread3.mem_signal.finished.connect(self.onStepChanged)
        thread3.mem_signal.error.connect(self.error)
        self.threadpool.start(thread3)

        thread4 = Thread(kotel_kr.start, 'kotel_kr')
        thread4.daemon = True
        thread4.mem_signal.finished.connect(self.onStepChanged)
        thread4.mem_signal.error.connect(self.error)
        self.threadpool.start(thread4)

    def error(self, error):
        self.ui.textEdit.setText(f'Помилка роботи скрипта. Запустіть скрипт наново')

    def onStepChanged(self, p):

        if self.threadpool.activeThreadCount() == 0:
            self.ui.textEdit.append(f'*** {p} *** парсинг завершений')
            self.ui.textEdit.append(f'Створюю зведені таблиці')
            table = PivotTable()
            table.mem_signal.finished.connect(self.end)
            self.threadpool.start(table)


        elif self.threadpool.activeThreadCount() == 3 or self.threadpool.activeThreadCount() == 4:
            self.ui.textEdit.setText(f'*** {p} *** парсинг завершений')
        else:
            self.ui.textEdit.append(f'*** {p} *** парсинг завершений')

    def end(self):
        os.remove('database.db')
        self.ui.textEdit.setText(f'>>> Скрипт завершив свою роботу <<<')
        self.ui.pushButton.setText('Почати')
        self.ui.pushButton.setEnabled(True)


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    myapp = MainProgram()
    myapp.show()
    sys.exit(app.exec())
