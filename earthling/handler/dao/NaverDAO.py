import os, sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import yaml
from handler.dao.BaseDAO import BaseDAO

# 해당 모듈을 추가하면 다른 곳에서 순환 종속 에러 발생할 수 잇음
from service.Logging import log


class NaverDAO(BaseDAO):

    def __init__(self):
        super().__init__("naver")


