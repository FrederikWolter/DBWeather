import datetime
import re
import requests
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
        Database.upsert(db.mongo_data_train, keys, dataset)

    except KeyError as e:
        # TODO logging
        print("Upsert Error")


def load_api_data(eva: str, current_time: datetime.datetime):
    # request api
    url = "https://reiseauskunft.bahn.de/bin/bhftafel.exe/dn"
    params = {
        # format expected from server (otherwise html page)
        "L": "vs_java",
        # impact not apparent (but error without)
        "start": "yes",
        # show arrival ['arr'] or departure ['dep'] information # TODO arr or dep, both?
        "boardType": "dep",
        # date to look at ['DD.MM.YY']
        "date": current_time.strftime("%d.%m.%y"),
        # time to look at ['HH:MM'] (server returns information around specified time)
        "time": current_time.strftime("%H:%M"),
        # train station eva-number
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
    i = 0

    for line in lines:
        # time - line
        if matches := re.fullmatch(r"(?P<hour>[0-2][0-9]):(?P<minute>[0-6][0-9])", string=line):
            hour = matches.group("hour")
            minute = matches.group("minute")
            timestamp = current_time.strftime("%Y-%m-%dT") + hour + ":" + minute + ":00+01:00"  # it's +1 now
            dataset["timestamp"] = timestamp

        # connection - line
        elif matches := re.fullmatch(r"(?P<type>[A-Za-z]+)([ \t]*)(?P<line>[A-Z0-9]+)", string=line):
            dataset["con_type"] = matches.group("type")
            dataset["con_line"] = matches.group("line")

        # changes - line
        elif matches := re.fullmatch(r"(?P<cancel>cancel)|(?P<zero>no|0)|(\+\s*(?P<delay>[0-9]+))", string=line):
            if matches.group("cancel"):
                dataset["delay"] = -1
            elif matches.group("zero"):
                dataset["delay"] = 0
            elif matches.group("delay"):
                dataset["delay"] = int(matches.group("delay"))
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
            # assert False

        # print(line)
    # endregion
    print(i)


###################################
# Main entry point of main_train.py
# call with "TODO Command cron?"
###################################
if __name__ == '__main__':
    database = Database()

    # get now
    now = datetime.datetime.now()

    load_api_data(eva="8000105", current_time=now)  # Frankfurt (Main) Hbf

    # database.mongo_data_train.delete_many({})  # delete all data from the db collection
    database.close()
