import datetime

CRITICAL = 50
ERROR = 40
WARNING = 30
INFO = 20
DEBUG = 10

class Logger:
    def __init__(self, level, log_file=None):
        self.level = level
        self.log_file = log_file


    def critical(self, message):
        if self.level <= CRITICAL:
            print(str(datetime.datetime.now()) + ' [CRITICAL] ' + message)
            if self.log_file is not None:
                with open(self.log_file, 'a') as file:
                    file.write(str(datetime.datetime.now()) + ' [CRITICAL] ' + message)


    def error(self, message):
        if self.level <= ERROR:
            print(str(datetime.datetime.now()) + ' [ERROR] ' + message)
            if self.log_file is not None:
                with open(self.log_file, 'a') as file:
                    file.write(str(datetime.datetime.now()) + ' [ERROR] ' + message)


    def warn(self, message):
        if self.level <= WARNING:
            print(str(datetime.datetime.now()) + ' [WARN] ' + message)
            if self.log_file is not None:
                with open(self.log_file, 'a') as file:
                    file.write(str(datetime.datetime.now()) + ' [WARN] ' + message)


    def info(self, message):
        if self.level <= INFO:
            print(str(datetime.datetime.now()) + ' [INFO] ' + message)
            if self.log_file is not None:
                with open(self.log_file, 'a') as file:
                    file.write(str(datetime.datetime.now()) + ' [INFO] ' + message)


    def debug(self, message):
        if self.level <= DEBUG:
            print(str(datetime.datetime.now()) + ' [DEBUG] ' + message)
            if self.log_file is not None:
                with open(self.log_file, 'a') as file:
                    file.write(str(datetime.datetime.now()) + ' [DEBUG] ' + message)
