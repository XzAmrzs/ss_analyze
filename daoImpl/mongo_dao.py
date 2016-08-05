from pymongo import MongoClient
from conf import conf

database_driver_host = conf.DATABASE_DRIVER_HOST
database_driver_port = conf.DATABASE_DRIVER_PORT
database_name = conf.DATABASE_NAME
collection_name = conf.COLLECTION_NAME


class MongoDao(object):
    def __init__(self, host, port, db_name):
        self.client = MongoClient(host, port, maxPoolsize=200)
        self.db = self.client.get_database(db_name)

    def update(self, col_name, *sql):
        col = self.db.get_collection(col_name)
        col.update_one(sql, True)


mongodao = MongoDao(database_driver_host, database_driver_port, database_name)

if __name__ == '__main__':
    mongodao.update('nodeHls', {'user': 'xzp'}, {'$set': {'flux': 123}})
