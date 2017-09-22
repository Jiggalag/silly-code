import datetime


class Logger:
    def __init__(self, logging_type, log_file=None):
        self.types = {
            'CRITICAL': 50,
            'ERROR': 40,
            'WARNING': 30,
            'INFO': 20,
            'DEBUG': 10
        }

        if logging_type not in self.types:
            print('Unregistered type of message: {}'.format(logging_type))
            # sys.exit(1)
            self.type = None
        else:
            self.type = logging_type
        self.log_file = log_file

    def critical(self, message):
        if self.types.get(self.type) <= self.types.get('CRITICAL'):
            print(str(datetime.datetime.now()) + ' [CRITICAL] ' + message)
            if self.log_file is not None:
                with open(self.log_file, 'a') as file:
                    file.write(str(datetime.datetime.now()) + ' [CRITICAL] ' + message)

    def error(self, message):
        if self.types.get(self.type) <= self.types.get('ERROR'):
            print(str(datetime.datetime.now()) + ' [ERROR] ' + message)
            if self.log_file is not None:
                with open(self.log_file, 'a') as file:
                    file.write(str(datetime.datetime.now()) + ' [ERROR] ' + message)

    def warn(self, message):
        if self.types.get(self.type) <= self.types.get('WARNING'):
            print(str(datetime.datetime.now()) + ' [WARN] ' + message)
            if self.log_file is not None:
                with open(self.log_file, 'a') as file:
                    file.write(str(datetime.datetime.now()) + ' [WARN] ' + message)

    def info(self, message):
        if self.types.get(self.type) <= self.types.get('INFO'):
            print(str(datetime.datetime.now()) + ' [INFO] ' + message)
            if self.log_file is not None:
                with open(self.log_file, 'a') as file:
                    file.write(str(datetime.datetime.now()) + ' [INFO] ' + message)

    def debug(self, message):
        if self.types.get(self.type) <= self.types.get('DEBUG'):
            print(str(datetime.datetime.now()) + ' [DEBUG] ' + message)
            if self.log_file is not None:
                with open(self.log_file, 'a') as file:
                    file.write(str(datetime.datetime.now()) + ' [DEBUG] ' + message)
