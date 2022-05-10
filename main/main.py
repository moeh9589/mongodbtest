from re import M
from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
load_dotenv(find_dotenv())

printer = pprint.PrettyPrinter()

password = os.environ.get("MONGODB_PWD")
connection_string = f"mongodb+srv://justinmo17:{password}@cluster0.gcbws.mongodb.net/pythontestDB?retryWrites=true&w=majority"

client = MongoClient(connection_string)
db = client.list_database_names()
firstdb = client.pythontestDB
collections = firstdb.list_collection_names()


def insert_test_doc():
    collection = firstdb.firstcollection
    test_document = {
        "name" : "justin",
        "type" : "test"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)

production = client.production
person_collection = production.person_collection


def create_documents():
    first_names = ["justin", "josh", "troy", "steph", "test"]
    last_names = ["moehlenpah", "moehlenpah", "moehlenpah", "moehlenpah", "test"]
    ages = [20, 20, 55, 54, 100]

    docs = []

    for first_name, last_name, age in zip(first_names, last_names, ages):
        doc = {"first_name": first_name, "last_name": last_name, "age": age}
        docs.append(doc)

    person_collection.insert_many(docs) 


def find_all_people():
    people = person_collection.find()

    for p in people:
        printer.pprint(p)


def find_person(name):
    person = person_collection.find_one({"first_name" : name})
    printer.pprint(person)


def count_all_people():
    count = person_collection.count_documents(filter={})
    print("Number of people: ", count)


def get_person_by_id(id_):
    from bson.objectid import ObjectId

    _id = ObjectId(id_)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)


def get_age_range(lowbound, upbound):
    query = {"$and": [
            {"age": {"$gte": lowbound}},
            {"age": {"$lte": upbound}}
        ]
    }
    
    people = person_collection.find(query).sort('age')
    for p in people:
        printer.pprint(p)


def project_columns():
    columns = {"_id" : 0, "first_name" : 1, "last_name" : 1}
    people = person_collection.find({}, columns)

    for p in people:
        printer.pprint(p)


def update_person_by_id(id_):
    from bson.objectid import ObjectId

    _id = ObjectId(id_)

    # all_updates = {
    #     "$set": {"new_field": True},  
    #     "$inc": {"age": 1},
    #     "$rename": {"first_name": "first", "last_name": "last"}
    # }

    person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})


def replace_one(id_):
    from bson.objectid import ObjectId

    _id = ObjectId(id_)
    
    new_doc = {
        "first_name": "new first name",
        "last_name" : "new last name",
        "age": 10

    }

    person_collection.replace_one({"_id" : _id}, new_doc)


def delete_by_id(id_):
    from bson.objectid import ObjectId

    _id = ObjectId(id_)

    person_collection.delete_one({"_id": _id})


address = {
    "_id": "29283yu4nkb2394",
    "street": "Fillmore Ct",
    "number": 13532,
    "city": "Thornton",
    "state": "Colorado",
    "country":"United States",
    "zip": "80241"
}
def add_address_embed(person_id, address):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person_collection.update_one({"_id": _id}, {"$addToSet": {'addresses': address}})


def add_address_relationship(person_id, address):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    address = address.copy()
    address['owner_id'] = _id
    address_collection = production.address

    address_collection.insert_one(address)





add_address_relationship('627aea8f474e38d6ce98e794', address)
# add_address_embed('627aea8f474e38d6ce98e793', address)
# delete_by_id('627aea8f474e38d6ce98e797')
# replace_one('627aca65aa45bed096f79e56')
# update_person_by_id('627aca65aa45bed096f79e56')
# project_columns()
# get_age_range(19,21)
# get_person_by_id('627aca65aa45bed096f79e56')    
# count_all_people()
# find_person("justin")
# find_all_people()
# create_documents()
# insert_test_doc()
# print(collections)
# print(connection_string)
