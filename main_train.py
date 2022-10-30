import requests
import json
import re
import datetime
from database import Database


# TODO comment
# TODO add logging


def save_to_db(db: Database, dataset: dict):
    try:
        print(dataset)  # TODO temp

        # get keys for this dataset
        keys = {
            "timestamp": dataset["timestamp"],
            "con_type": dataset["con_type"],
            "con_line": dataset["con_line"]
        }

        # save (update or insert) the dataset to the database
        db.upsert(db.mongo_data_train, keys, dataset)
    except KeyError as e:
        # TODO logging
        print("Upsert Error")


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
    i =0

    for line in lines:
        # time - line
        if matches := re.fullmatch(r"(?P<hour>[0-2][0-9]):(?P<minute>[0-6][0-9])", string=line):
            hour = matches.group("hour")
            minute = matches.group("minute")
            timestamp = now.strftime("%Y-%m-%dT") + hour + ":" + minute + ":00+02:00"
            dataset["timestamp"] = timestamp

        # connection - line
        elif matches := re.fullmatch(r"(?P<type>[A-Za-z]+)([ \t]*)(?P<line>[A-Z0-9]+)", string=line):
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
            save_to_db(db=database, dataset=dataset)
            i += 1
            dataset = {}

        # error - line not recognized
        else:
            print("Error: ", line, dataset)
            #assert False

        #print(line)
    # endregion
    print(i)


###################################
# Main entry point of main_train.py
# call with "TODO Command cron?"
###################################
if __name__ == '__main__':
    database = Database()
    load_api_data(eva="8000105")  # Frankfurt (Main) Hbf

    #database.mongo_data_train.delete_many({})  # delete all data from the db collection
    database.close()
