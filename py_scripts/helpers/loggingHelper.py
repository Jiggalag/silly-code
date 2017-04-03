import datetime

CRITICAL = 50
ERROR = 40
WARNING = 30
INFO = 20
DEBUG = 10

class Logger:
    def __init__(self, level):
        self.level = level


    def critical(self, message):
        if self.level <= CRITICAL:
            print(str(datetime.datetime.now()) + ' [CRITICAL] ' + message)


    def error(self, message):
        if self.level <= ERROR:
            print(str(datetime.datetime.now()) + ' [ERROR] ' + message)


    def warn(self, message):
        if self.level <= WARNING:
            print(str(datetime.datetime.now()) + ' [WARN] ' + message)


    def info(self, message):
        if self.level <= INFO:
            print(str(datetime.datetime.now()) + ' [INFO] ' + message)


    def debug(self, message):
        if self.level <= DEBUG:
            print(str(datetime.datetime.now()) + ' [DEBUG] ' + message)