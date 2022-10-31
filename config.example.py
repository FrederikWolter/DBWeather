# TODO comment
# TODO sync with config.py

# Config file for the DBWeather Application

# config values for database connection
mongodb = {
    # hostname of the mongodb
    "host": "localhost",
    # default port for the mongodb
    "port": "27017",
    # username for the mongodb
    "user": "root",
    # password of the user for the mongodb
    "pwd": "example",
    # database name used by the application
    "db": "DBWeather",

    # collections
    "data_train": "data_train",
    "data_weather": "data_weather"
}
