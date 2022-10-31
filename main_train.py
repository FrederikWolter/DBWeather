import datetime
import re
import requests
import logging
from database import Database
from enum import Enum


# TODO comment


class BoardType(Enum):
    ARRIVAL = "arr"
    DEPARTURE = "dep"


def save_to_db(db: Database, dataset: dict):
    try:
        logger.debug("Saving to database '%s'", dataset)

        # get keys for this dataset
        keys = {
            "board_type": dataset["board_type"],
            "timestamp": dataset["timestamp"],
            "con_type": dataset["con_type"],
            "con_line": dataset["con_line"]
        }

        # save (update or insert) the dataset to the database
        Database.upsert(db.mongo_data_train, keys, dataset)

    except KeyError:
        logger.exception("Given dataset missing key(s):")
        print("Upsert Error")


def load_api_data(eva: str, current_time: datetime.datetime, board_type: BoardType):
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
        assert False

    # region process answer
    answer = r.text

    # get individual lines
    lines = answer.splitlines()
    lines = lines[1:]  # drop first line with header information
    logger.debug("Result has %s line, calculated %s datasets", len(lines), len(lines) / 3)

    dataset = {"board_type": board_type.value}

    # parse lines
    for line in lines:
        # time - line
        if matches := re.fullmatch(r"(?P<hour>[0-2][0-9]):(?P<minute>[0-6][0-9])", string=line):
            hour = matches.group("hour")
            minute = matches.group("minute")
            dataset["timestamp"] = current_time.strftime("%Y-%m-%dT") + hour + ":" + minute + ":00+01:00"  # it's +1 now

        # connection - line
        elif matches := re.fullmatch(r"(?P<type>[A-Za-z]+)([ \t]*)(?P<line>[A-Z0-9]+)", string=line):
            dataset["con_type"] = matches.group("type")
            dataset["con_line"] = matches.group("line")

        # changes - line
        elif matches := re.fullmatch(r"(?P<cancel>cancel)|(?P<zero>no|0)|(\+\s*(?P<delay>[0-9]+))", string=line):
            if matches.group("cancel"):
                dataset["delay"] = -1   # -1 representing canceled
            elif matches.group("zero"):
                dataset["delay"] = 0
            elif matches.group("delay"):
                dataset["delay"] = int(matches.group("delay"))
            else:
                logger.error("Not recognized changes line: '%s'", line)

            # save processed dataset
            save_to_db(db=database, dataset=dataset)
            dataset = {"board_type": board_type.value}  # reset dataset

        # error - line not recognized
        else:
            logger.error("Not recognized line: '%s' with dataset '%s'", line, dataset)
    # endregion


###################################
# Main entry point of main_train.py
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
    logger = logging.getLogger("train")
    logger.info("Start main_train execution ...")

    # get now
    now = datetime.datetime.now()

    # setup database connection and load data
    database = Database()
    logger.debug("Start processing Frankfurt (Main) Hbf arrivals ...")
    load_api_data(eva="8000105", current_time=now, board_type=BoardType.ARRIVAL)
    logger.debug("Start processing Frankfurt (Main) Hbf departures ...")
    load_api_data(eva="8000105", current_time=now, board_type=BoardType.DEPARTURE)
    database.close()
    # database.mongo_data_train.delete_many({})  # delete all data from the db collection # TODO

    logger.info("finished ###########################################")
