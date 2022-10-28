import pymongo
import requests
import json
import re
import datetime
import database

# TODO comment
# TODO add logging


def save_to_db(db_client: pymongo.MongoClient, dataset: dict):
    print(dataset)
    # TODO implement


def load_api_data(eva: str, db_client: pymongo.MongoClient):
    # get now
    now = datetime.datetime.now()

    # connect to database  # TODO
    mydb = db_client["mydatabase"]
    print(db_client.list_database_names())

    # request api
    url = "https://reiseauskunft.bahn.de/bin/bhftafel.exe/dn"
    params = {
        "L": "vs_java",         # TODO what?
        "start": "yes",         # TODO what?
        "boardType": "dep",     # TODO arr or dep
        "date": "28.10.22",     # TODO make dynamic
        "time": "19:00",        # TODO make dynamic
        "input": eva
    }
    headers = { }
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
    lines = lines[1:]               # drop first line including the header information

    dataset = {}

    for line in lines:
        # time - line
        if re.fullmatch(pattern=r"[0-2][0-9]:[0-6][0-9]", string=line) is not None:
            hour = line.split(":")[0]
            minute = line.split(":")[1]
            timestamp = now.strftime("%Y-%m-%dT") + hour + ":" + minute + ":00+02:00"
            dataset["timestamp"] = timestamp

        # connection - line
        elif re.fullmatch(pattern=r"[A-Za-z]+[ \t]*[A-Z0-9]+", string=line) is not None:
            con_type = line.split(sep=" ")[0]
            con_line = line.split(sep=" ")[-1]
            dataset["con_type"] = con_type
            dataset["con_line"] = con_line

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
            save_to_db(db_client=db_client, dataset=dataset)
            dataset = {}

        # error - line not recognized
        else:
            print("Error")
            assert False

        # print(line)

    print(json.dumps({"c": 0, "b": 0, "a": 0}, sort_keys=True))  # TODO test
    # endregion


#################################
# Main entry point of main_db.py
#################################
if __name__ == '__main__':
    database = database.connect()
    load_api_data(eva="8000105", db_client=database)    # Frankfurt (Main) Hbf
