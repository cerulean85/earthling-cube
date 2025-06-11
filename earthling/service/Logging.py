'''

python 내장모듈 logging...

ㅇ 로그 종류.
DEBUG
INFO
WARNING
ERROR

사용법:

from Logging import log <= 모듈 츄가

EXAMPLE)
print("ComAssistant가 시작되었습니다.") <= 로그 찍기
print(f"Manager로부터 받은 echo 메시지: {message}")

일단위로 로그 찍음...

'''

import logging
import datetime
import logging.handlers
class Logging:    
    _instance = None
    logger = None
    def __init__(self):

        # logging.basicConfig()
        # logging.basicConfig(
        #     filename="example.log",
        #     format='%(asctime)s %(levelname)s:%(message)s',
        #     level=logging.DEBUG,
        #     datefmt='%m/%d/%Y %I:%M:%S %p',)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter(
            fmt='[%(asctime)s] (%(levelname)s) => %(message)s',
            datefmt= '%Y-%m-%d %H:%M:%S')

        streamhandler = logging.StreamHandler()
        streamhandler.setFormatter(formatter)
        self.logger.addHandler(streamhandler)

        # filehandler = logging.FileHandler('logs/log_{:%Y%m%d}.log'.format(datetime.datetime.now()), encoding='utf-8')
        # filehandler.setFormatter(formatter)
        # self.logger.addHandler(filehandler)

        filename = 'logs/logfile.log'  # 파일명 ...
        timedfilehandler = logging.handlers.TimedRotatingFileHandler(filename=filename, when='midnight', interval=1, encoding='utf-8')
        timedfilehandler.setFormatter(formatter)
        timedfilehandler.suffix = "%Y%m%d"

        self.logger.addHandler(timedfilehandler)



    @classmethod
    def getInstance(cls):
        if not cls._instance:
            cls._instance = Logging()
        return cls._instance


log = Logging.getInstance().logger
# def log():
#     logger = Logging.getInstance().logger
#     return logger