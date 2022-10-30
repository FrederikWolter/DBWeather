import requests
import json
import re
import datetime
from database import Database


# TODO comment
# TODO add logging


def save_to_db(db: Database, dataset: dict):
    print(dataset)  # TODO temp

    # get keys for this dataset
    keys = {
        "timestamp": dataset["timestamp"],
        "con_type": dataset["con_type"],
        "con_line": dataset["con_line"]
    }

    # save (update or insert) the dataset to the database
    db.upsert(db.mongo_data_train, keys, dataset)


def load_api_data(eva: str):
    # get now
    now = datetime.datetime.now()

    # request api
    url = "https://reiseauskunft.bahn.de/bin/bhftafel.exe/dn"
    params = {
        "L": "vs_java",  # TODO what?
        "start": "yes",  # TODO what?
        "boardType": "dep",  # TODO arr or dep
        "date": "30.10.22",  # TODO make dynamic
        "time": "20:00",  # TODO make dynamic
        "input": eva
    }
    headers = {}
    r = requests.get(url=url, params=params, headers=headers, timeout=10)

    # request successful?
    if r.status_code != 200:
        assert False

    # get answer text
    answer = r.text
    # print(answer)

    # region process answer
    # get individual lines
    lines = answer.splitlines()
    lines = lines[1:]  # drop first line including the header information

    dataset = {}

    for line in lines:
        # time - line
        if matches := re.fullmatch("(?P<hour>[0-2][0-9]):(?P<minute>[0-6][0-9])", string=line):
            hour = matches.group("hour")
            minute = matches.group("minute")
            timestamp = now.strftime("%Y-%m-%dT") + hour + ":" + minute + ":00+02:00"
            dataset["timestamp"] = timestamp

        # connection - line
        elif matches := re.fullmatch("(?P<type>[A-Za-z]+)([ \t]*)(?P<line>[A-Z0-9]+)", string=line):
            dataset["con_type"] = matches.group("type")
            dataset["con_line"] = matches.group("line")

        # changes - line
        elif re.fullmatch(pattern=r"cancel|no|0|\+\s[0-9]+", string=line) is not None:
            if line == "cancel":
                dataset["delay"] = -1
            elif line == "no" or line == "0":
                dataset["delay"] = 0
            elif line.startswith("+"):
                delay = re.search(pattern="[0-9]+", string=line).group(0)
                dataset["delay"] = int(delay)
            else:
                print("Error ln 3")
                assert False

            # save processed dataset
            save_to_db(db=dataset, dataset=dataset)
            dataset = {}

        # error - line not recognized
        else:
            print("Error")
            assert False

        # print(line)

    print(json.dumps({"c": 0, "b": 0, "a": 0}, sort_keys=True))  # TODO test
    # endregion


###################################
# Main entry point of main_train.py
# call with "TODO Command cron?"
###################################
if __name__ == '__main__':
    database = Database()
    load_api_data(eva="8000105")  # Frankfurt (Main) Hbf

    #database_client.db.data.delete_many({})  # delete all data from the db collection
    database.close()
