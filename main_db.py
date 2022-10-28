import pymongo


def main():
    myclient = pymongo.MongoClient("mongodb://root:example@localhost:27017/")

    mydb = myclient["mydatabase"]

    print(myclient.list_database_names())


if __name__ == '__main__':
    main()
