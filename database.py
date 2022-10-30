import config as cfg
import pymongo


# Connect to a mongodb-database and return the pymongo.MongoClient
def connect():
    # Build the connection string
    host = "mongodb://" + cfg.mongodb["user"] + ":" + cfg.mongodb["pwd"] + "@" + cfg.mongodb["host"] + ":" + str(
        cfg.mongodb["port"])
    return pymongo.MongoClient(host=host)


"""Inserts or updates an document inside a passed collection

:param collection: A pymongo.collection object
:param query: A JSON-formatted-object including the key of the item, which should be inserted or updated
:param update: A JSON-formatted-object including all the items, which should be updated
:returns: The updated or inserted item

Example usage:
    query = {
        "name": "Test"
    }
    update = {
        "message": "First Document updated"
    }
    database.insert_or_update(collection, query, update)
"""
def insert_or_update(collection: pymongo.collection, query: dict, update: dict):
    return collection.update_one(query, {"$set": update}, upsert=True)
