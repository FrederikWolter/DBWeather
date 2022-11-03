import datetime
import logging

import requests

from database import Database


# TODO comment

def save_to_db(db: Database, dataset: dict) -> None:
    try:
        logger.debug("Saving to database '%s'", dataset)

        # get keys for this dataset
        keys = {
            "timestamp": dataset["timestamp"],
            "lat": dataset["lat"],
            "lon": dataset["lon"]
        }

        # save (update or insert) the dataset to the database
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


def load_api_data(lat: float, lon: float, current_time: datetime.datetime) -> None:
    # request api
    url = "https://api.brightsky.dev/weather"
    params = {
        # date to look at ['YYYY-MM-DD']
        "date": current_time.strftime("%Y-%m-%d"),
        # lon of city XX.XX
        "lon": lon,
        # lat of city XX.XX
        "lat": lat,
        # timezone of city
        "tz": "Europe/Berlin"
    }
    headers = {}
    r = requests.get(url=url, params=params, headers=headers, timeout=20)

    # request successful?
    if r.status_code != 200:
        logger.critical("Request return unexpected exit code '%s'", r.status_code)
        assert False

    # region process answer
    answer = r.json()

    weather = answer['weather']  # get only weather data
    logger.info("Result has %s elements", len(weather))

    # save processed dataset
    for dataset in weather:
        dataset['lat'] = lat
        dataset['lon'] = lon
        save_to_db(db=database, dataset=dataset)

    # endregion


###################################
# Main entry point of main_weather.py
# call with "TODO Command cron?"
###################################
if __name__ == '__main__':
    # setup logging
    logging.basicConfig(
        filename='execution.log',
        filemode="a",
        format='%(asctime)s %(levelname)-7s %(name)s: %(message)s',
        encoding='utf-8',
        level=logging.DEBUG
    )
    logger = logging.getLogger("weather")
    logger.info("Start main_weather execution ...")

    # get now
    now = datetime.datetime.now()

    # setup counter
    global num_inserted
    num_inserted = 0
    global num_updated
    num_updated = 0
    global num_unchanged
    num_unchanged = 0

    # setup database connection and load data
    database = Database()
    logger.debug("Start processing Frankfurt (Main) weather data ...")
    load_api_data(lat=50.05, lon=8.6, current_time=now)
    database.close()
    # database.mongo_data_weather.delete_many({})  # delete all data from the db collection # TODO

    logger.info("finished: %s inserted, %s updated, %s unchanged", num_inserted, num_updated, num_unchanged)
    logger.info("###########################################")
