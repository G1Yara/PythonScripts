import json
import os
import requests
from bson import ObjectId
from pymongo import MongoClient

mongo_uri = os.getenv("mong_uri")
preload_channel_api = os.getenv("preload_channel_api")
auth_token = os.getenv("auth_token")
acadia_db = 'Acadia'
organization_collection_name = "organization"

vps_db = 'vps'
preload_collection_name = 'preloadChannel'


def get_mongodb_client(mongo_uri):
    client = MongoClient(mongo_uri)
    return client


def get_mongo_collection(db_client, db_name, collection_name):
    collection_instance = None

    try:
        db_instance = db_client[db_name]
        collection_instance = db_instance[collection_name]
    except Exception as e:
        print(e.__str__())

    return collection_instance


if __name__ == '__main__':

    mong_client = get_mongodb_client(mongo_uri)

    organization_collection = get_mongo_collection(mong_client, acadia_db, organization_collection_name)

    for org_document in organization_collection.find():

        organization_id = org_document["_id"]
        organization_name = org_document["name"]

        print(f"organization id : {organization_id}, name : {organization_name}")

        url = preload_channel_api + "/" + str(organization_id)

        headers = {
            "Authorization": auth_token,
            "Organization": str(organization_id)
        }
        response = requests.get(url, headers=headers)

        print(f"response from api : {response}")

        if response.status_code != 200:
            continue

        if response.status_code == 200:
            json_response = json.loads(response.text)
            print(f"json response: {json_response}")
            data = json_response["data"]
            for obj in data:
                if obj["status"] == 'ACTIVE':
                    number_of_slots_count = obj["numberOfSlots"]

                    print(f"preload slots count: {number_of_slots_count}")

                    updated_collection = organization_collection.find_one_and_update(
                        {"_id": ObjectId(organization_id)},
                        {"$set": {"preloadSlotsCount": number_of_slots_count}},
                        upsert=True)

                    print(f"updated collection details: {updated_collection.values()}")
        break
