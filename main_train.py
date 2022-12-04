import datetime
import logging
import re
from enum import Enum

import requests
from pytz import timezone

from database import Database


class BoardType(Enum):
    """
    Enum representing the possible board types.
    """
    ARRIVAL = "arr"
    DEPARTURE = "dep"


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
            "eva": dataset["eva"],
            "board_type": dataset["board_type"],
            "timestamp": dataset["timestamp"],
            "con_type": dataset["con_type"],
            "con_line": dataset["con_line"]
        }

        # save (upsert) the dataset to the database
        result = database.upsert(collection=db.mongo_data_train, query=keys, update=dataset)

        # update stats
        global num_inserted
        num_inserted += 1 - result.matched_count
        global num_updated
        num_updated += result.modified_count
        global num_unchanged
        num_unchanged += not result.modified_count and result.matched_count

    except KeyError:
        logger.exception("Given dataset missing key(s):")


def _load_api_data(eva: int, current_time: datetime.datetime, board_type: BoardType) -> None:
    """
    Load data from API and save it to the database.

    :param eva: ID of the train station
    :param current_time: current datetime
    :param board_type: board type (arr or dep)
    :return: None
    """

    # request api
    url = "https://reiseauskunft.bahn.de/bin/bhftafel.exe/dn"
    params = {
        # format expected from server (otherwise html page)
        "L": "vs_java",
        # impact not apparent (but error without)
        "start": "yes",
        # show arrival ['arr'] or departure ['dep'] information
        "boardType": board_type.value,
        # date to look at ['DD.MM.YY']
        "date": current_time.strftime("%d.%m.%y"),
        # time to look at ['HH:MM'] (server returns information around specified time)
        "time": current_time.strftime("%H:%M"),
        # train station eva-number
        "input": eva
    }
    headers = {}
    r = requests.get(url=url, params=params, headers=headers, timeout=20)

    # request successful?
    if r.status_code != 200:
        logger.critical("Request return unexpected exit code '%s'", r.status_code)
        assert False  # exit with a big bang

    # region process answer
    answer = r.text

    # get individual lines
    lines = answer.splitlines()
    lines = lines[1:]  # drop first line with header information
    logger.info("Result eva=%s type=%s has %s lines, calc %s sets", eva, board_type.value, len(lines), len(lines) / 3)

    # prefill dataset
    dataset = {
        "eva": eva,
        "board_type": board_type.value
    }

    # parse lines
    for line in lines:
        # time - line
        if matches := re.fullmatch(r"(?P<hour>[0-2][0-9]):(?P<minute>[0-6][0-9])", string=line):
            hour = int(matches.group("hour"))
            minute = int(matches.group("minute"))
            dataset["timestamp"] = current_time.replace(hour=hour, minute=minute, second=0, microsecond=0).isoformat()

        # connection - line
        elif matches := re.fullmatch(r"(?P<type>[A-Za-z]+)([ \t]*)(?P<line>[A-Z0-9]+)", string=line):
            dataset["con_type"] = matches.group("type")
            dataset["con_line"] = matches.group("line")

        # changes - line
        elif matches := re.fullmatch(r"(?P<cancel>cancel)|(?P<zero>no|0)|(\+\s*(?P<delay>[0-9]+))", string=line):
            if matches.group("cancel"):
                dataset["delay"] = -1  # -1 representing canceled
            elif matches.group("zero"):
                dataset["delay"] = 0
            elif matches.group("delay"):
                dataset["delay"] = int(matches.group("delay"))
            else:
                logger.error("Not recognized changes line: '%s'", line)

            # save processed dataset
            _save_to_db(db=database, dataset=dataset)

            # reset dataset
            dataset = {
                "eva": eva,
                "board_type": board_type.value
            }

        # error - line not recognized
        else:
            logger.error("Not recognized line: '%s' with dataset '%s'", line, dataset)
    # endregion


###################################
# Main entry point of main_train.py
# call with cronjob:
# 0 * * * * /usr/local/bin/python3.10 /home/bigdata/DBWeather/main_train.py
# 30 * * * * /usr/local/bin/python3.10 /home/bigdata/DBWeather/main_train.py
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
    logger = logging.getLogger("train")
    logger.info("Start main_train execution ...")

    # setup global counter
    num_inserted = 0
    num_updated = 0
    num_unchanged = 0

    # setup database connection and load data
    database = Database()

    logger.debug("Start processing Frankfurt (Main) Hbf arrivals ...")
    _load_api_data(eva=8000105, current_time=now, board_type=BoardType.ARRIVAL)

    logger.debug("Start processing Frankfurt (Main) Hbf departures ...")
    _load_api_data(eva=8000105, current_time=now, board_type=BoardType.DEPARTURE)

    logger.debug("Start processing Mannheim Hbf arrivals ...")
    _load_api_data(eva=8000244, current_time=now, board_type=BoardType.ARRIVAL)

    logger.debug("Start processing Mannheim Hbf departures ...")
    _load_api_data(eva=8000244, current_time=now, board_type=BoardType.DEPARTURE)

    database.close()

    logger.info("finished: %s inserted, %s updated, %s unchanged", num_inserted, num_updated, num_unchanged)
    logger.info("###########################################")
