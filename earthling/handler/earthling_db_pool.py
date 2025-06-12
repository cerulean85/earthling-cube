import os, sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from connector.DBPoolConnector import DBPoolConnector, execute


class FirstEarthlingDBPool(DBPoolConnector):
    name = "kkennibdb"

    def __init__(self):
        self.getDBOption(self.name)
        if not FirstEarthlingDBPool._instance:
            self.pool = self.getPool()

    @classmethod
    def getInstance(cls):
        if not cls._instance:
            cls._instance = FirstEarthlingDBPool()
        return cls._instance


# class SecondaryEarthlingDBPool(MySQLPoolConnector):
#     name = 'textom_cn'
#     def __init__(self):
#         self.getDBOption(self.name)
#         if not SecondaryEarthlingDBPool._instance:
#             self.pool = self.getPool()

#     @classmethod
#     def getInstance(cls):
#         if not cls._instance:
#             cls._instance = SecondaryEarthlingDBPool()
#         return cls._instance


def exec(query, country="kr"):
    alter_pool = None
    if country == "kr":
        alter_pool = FirstEarthlingDBPool
    # if country == 'cn':
    #     alter_pool = SecondaryEarthlingDBPool
    return execute(query, alter_pool)
