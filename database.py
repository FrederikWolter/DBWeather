import config as cfg
import pymongo

# TODO comment
# TODO add logging


class Database:
    def __init__(self):
        """
        Connect to the mongodb-database and create references to the relevant collections.
        """

        # build the connection string
        host = "mongodb://" + cfg.mongodb["user"] + ":" + cfg.mongodb["pwd"] + "@" + cfg.mongodb["host"] + ":" + cfg.mongodb["port"]

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

    def upsert(self, collection: pymongo.collection, query: dict, update: dict):
        """
        Saves a dataset inside a passed collection.
        Depending on the result of the query a new dataset is added or an existing is updated.

        Example usage:
            - query = { "name": "Test" }
            - update = { "name": "Test", "message": "First Document updated" }
            - Is there a dataset with name = Test? Update this dataset or insert it.

        :param collection: The collection to be used
        :param query: A dict including keys of the item to upsert
        :param update: A dict including all the data, which should be updated
        :returns: The updated or inserted item
        """

        return collection.update_one(query, {"$set": update}, upsert=True)

    def close(self) -> None:
        """
        Close all connections to the mongodb-database.upsert
        :return: None
        """
        self.mongo_client.close()
