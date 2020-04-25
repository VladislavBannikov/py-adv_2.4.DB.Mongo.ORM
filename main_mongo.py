import csv
from datetime import datetime
from pymongo import MongoClient, errors
from pprint import pprint
import re


def connect_mongo():
    try:
        client = MongoClient(host=['localhost:27017'], serverSelectionTimeoutMS = 2000)
        client.server_info()
        return client
    except errors.ServerSelectionTimeoutError as err:
        print("Error: ", err)


def read_data(db_coll):
    """импорт данных из csv файла;"""
    with open("artists.csv", encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=",")
        artists_temp =[]
        for r in reader:
            r["Цена"] = int(r.get("Цена"))
            r["Дата"] = datetime.strptime(f"{r.get('Дата')}.2020", '%d.%m.%Y')
            artists_temp.append(r)
        db_coll.insert_many(artists_temp)


def find_cheapest(artists_collection):
    """отсортировать билеты из базы по возрастанию цены;"""
    result = artists_collection.find({}).sort('Цена', 1)
    return list(result)


def find_by_name(name):
    """найти билеты по исполнителю, где имя исполнителя может быть задано не полностью, и вернуть их по возрастанию цены."""
    regex = re.compile(re.escape(name))
    result = artists_collection.find({'Исполнитель': {"$regex": regex}}).sort('Цена', 1)

    return list(result)


if __name__ == '__main__':
    client = connect_mongo()
    artist_db = client['artist']
    artists_collection = artist_db['artist']
    # read_data(artists_collection)
    print("========find_cheapest=========")
    pprint(find_cheapest(artists_collection))
    print("========find_by_name=========")
    pprint(find_by_name('S'))

