import datetime
import logging

import requests
from pytz import timezone

from database import Database


def _save_to_db(db: Database, dataset: dict) -> None:
    """
    Save a captured dataset to the database.

    :param db: database to be used
    :param dataset: dataset to be saved
    :return: None
    """

    try:
        logger.debug("Saving to database '%s'", dataset)

        # get keys for this dataset
        keys = {
            "timestamp": dataset["timestamp"],
            "lat": dataset["lat"],
            "lon": dataset["lon"]
        }

        # save (upsert) the dataset to the database
        result = database.upsert(collection=db.mongo_data_weather, query=keys, update=dataset)

        # update stats
        global num_inserted
        num_inserted += 1 - result.matched_count
        global num_updated
        num_updated += result.modified_count
        global num_unchanged
        num_unchanged += not result.modified_count and result.matched_count

    except KeyError:
        logger.exception("Given dataset missing key(s):")


def _load_api_data(lat: float, lon: float, current_time: datetime.datetime) -> None:
    """
    Load data from API and save it to the database.

    :param lat: latitude of position
    :param lon: longitude of position
    :param current_time: current datetime
    :return: None
    """

    # request api
    url = "https://api.brightsky.dev/weather"
    params = {
        # date to look at ['YYYY-MM-DD']
        "date": current_time.strftime("%Y-%m-%d"),
        # lon of city [XX.XX]
        "lon": lon,
        # lat of city [XX.XX]
        "lat": lat,
        # timezone of city
        "tz": "Europe/Berlin"
    }
    headers = {}
    r = requests.get(url=url, params=params, headers=headers, timeout=20)

    # request successful?
    if r.status_code != 200:
        logger.critical("Request return unexpected exit code '%s'", r.status_code)
        assert False  # exit with a big bang

    # region process answer
    answer = r.json()

    # get only weather data
    weather = answer['weather']
    logger.info("Result lat=%s lon=%s has %s elements", lat, lon, len(weather))

    # save processed dataset
    for dataset in weather:
        dataset["lat"] = lat
        dataset["lon"] = lon
        _save_to_db(db=database, dataset=dataset)
    # endregion


###################################
# Main entry point of main_weather.py
# call with cronjob:
# 0 * * * * /usr/local/bin/python3.10 /home/bigdata/DBWeather/main_weather.py
# 30 * * * * /usr/local/bin/python3.10 /home/bigdata/DBWeather/main_weather.py
###################################
if __name__ == '__main__':
    # get now
    now = datetime.datetime.now(tz=timezone("Europe/Berlin"))

    logDate = now.strftime("%d-%m-%y")

    # setup logging
    logging.basicConfig(
        filename='execution_' + logDate + '.log',
        filemode="a",
        format='%(asctime)s %(levelname)-7s %(name)s: %(message)s',
        encoding='utf-8',
        level=logging.INFO
    )
    logger = logging.getLogger("weather")
    logger.info("Start main_weather execution ...")

    # setup global counter
    num_inserted = 0
    num_updated = 0
    num_unchanged = 0

    # setup database connection and load data
    database = Database()

    logger.debug("Start processing Frankfurt (Main) weather data ...")
    _load_api_data(lat=50.05, lon=8.6, current_time=now)

    logger.debug("Start processing Mannheim weather data ...")
    _load_api_data(lat=49.5, lon=8.48, current_time=now)

    database.close()

    logger.info("finished: %s inserted, %s updated, %s unchanged", num_inserted, num_updated, num_unchanged)
    logger.info("###########################################")
