import config as cfg
import pymongo


def connect():
    host = "mongodb://"+cfg.mongodb["user"]+":"+cfg.mongodb["pwd"]+"@"+cfg.mongodb["host"]+":"+str(cfg.mongodb["port"])
    return pymongo.MongoClient(host=host)
