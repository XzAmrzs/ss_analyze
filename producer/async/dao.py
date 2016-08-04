# coding=utf-8
from pymongo import MongoClient

def mongo_insert(tb,db):
    db.user.insert({})

client = MongoClient("127.0.0.1",27017)
db = client.hls
content = db.raw_hls


