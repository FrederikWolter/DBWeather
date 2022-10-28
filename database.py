import config as cfg
import pymongo


# Connect to a mongodb-database and return the pymongo.MongoClient
def connect():
    # Build the connection string
    host = "mongodb://" + cfg.mongodb["user"] + ":" + cfg.mongodb["pwd"] + "@" + cfg.mongodb["host"] + ":" + str(
        cfg.mongodb["port"])
    return pymongo.MongoClient(host=host)
