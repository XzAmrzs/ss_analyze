# coding=utf-8
from pymongo import MongoClient
from conf import conf

database_driver_host = conf.DATABASE_DRIVER_HOST
database_driver_port = conf.DATABASE_DRIVER_PORT
database_name = conf.DATABASE_NAME
collection_name = conf.COLLECTION_NAME

# 这里需要实现序列化的魔法方法才可以被spark程序所使用，参考地址
# http://stackoverflow.com/questions/3375443/cant-pickle-loggers
# https://groups.google.com/forum/#!topic/celery-users/E3CurhqxNDI
# http://www.jb51.net/article/57661.htm

class MongoDao(object):
    def __init__(self, host, port, db_name):
        self.client = MongoClient(host, port, maxPoolsize=200)
        self.db = self.client.get_database(db_name)

    def update(self, col_name, *sql):
        col = self.db.get_collection(col_name)
        col.update_one(sql, True)

    def __setstate__(self):
        pass

    def __getstate__(self):
        return self.db


mongodao = MongoDao(database_driver_host, database_driver_port, database_name)

if __name__ == '__main__':
    mongodao.update('nodeHls', {'user': 'xzp'}, {'$set': {'flux': 123}})
