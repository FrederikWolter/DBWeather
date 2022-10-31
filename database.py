import logging
import pymongo
import config as cfg


# TODO comment

class Database:
    def __init__(self):
        """
        Connect to the mongodb-database and create references to the relevant collections.
        """

        # get logger
        self.logger = logging.getLogger("database")

        # build the connection string
        host = "mongodb://" + cfg.mongodb["user"] + ":" + cfg.mongodb["pwd"] + "@" + \
               cfg.mongodb["host"] + ":" + cfg.mongodb["port"]

        # build connection
        self.mongo_client = pymongo.MongoClient(
            host=host,
            document_class=dict,
            tz_aware=True,
            connect=True,
        )
        self.mongo_db = self.mongo_client[cfg.mongodb["db"]]

        # get collections
        self.mongo_data_train = self.mongo_db[cfg.mongodb["data_train"]]
        self.mongo_data_weather = self.mongo_db[cfg.mongodb["data_weather"]]

    @staticmethod
    def upsert(collection: pymongo.collection.Collection, query: dict, update: dict) -> None:
        """
        Saves a dataset inside a passed collection.
        Depending on the result of the query a new dataset is added or an existing is updated.

        Example usage:
            - query = { "name": "Test" }
            - update = { "name": "Test", "message": "First Document updated" }
            - Is there a dataset with name = Test? Update this dataset or insert it.

        :param collection: collection to be used
        :param query: dict with keys of the item to upsert
        :param update: dict with all data, which should be updated
        """

        collection.update_one(query, {"$set": update}, upsert=True)
        # TODO logging use result

    def close(self) -> None:
        """
        Close all connections to the mongodb-database.upsert
        :return: None
        """
        self.mongo_client.close()
