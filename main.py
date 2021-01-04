try:
    import os
    import pandas as pd
    import sys
    import pymongo
    import json
    import urllib.parse

    from pymongo import MongoClient
    from bson.objectid import ObjectId

    print("All Modules Loaded Successfully.")
except Exception as e:
    print("Module Import Error: {}".format(e))


def main():
    """
    Coonection to Mongo DB for Writing, Updating, Reading and Deleting.
    """
    try:
        with open("./config.json") as f:
            parameters_config = json.load(f)
    except FileNotFoundError as e:
        print(e)

    MONGO_INITDB_ROOT_USERNAME = parameters_config["MONGO_INITDB_ROOT_USERNAME"]
    MONGO_INITDB_ROOT_PASSWORD = parameters_config["MONGO_INITDB_ROOT_PASSWORD"]
    PORT = parameters_config["PORT"]

    username = urllib.parse.quote_plus(MONGO_INITDB_ROOT_USERNAME)
    password = urllib.parse.quote_plus(MONGO_INITDB_ROOT_PASSWORD)

    MONGO_CONNECTION_URL = parameters_config["MONGO_CONNECTION_URL"].format(
        username, password
    )
    print(MONGO_CONNECTION_URL)
    # url is just an example (your url will be different)

    try:
        client = pymongo.MongoClient(host=MONGO_CONNECTION_URL)
    except InvalidURI as e:
        print("connection error", e)
        raise
    except Exception as e:
        print("Unexpected error:")

    # Getting All DB Names
    print(client.list_database_names())

    # Geeting all collection/table names
    DBNAME = parameters_config["DBNAME"]
    print(client[DBNAME].list_collection_names())
    COLLECTION_NAME = parameters_config["COLLECTION_NAME"]

    # 1. Inserting
    # Inserting with unique identifier specified by the application.
    try:
        client[DBNAME][COLLECTION_NAME].insert_one(
            {
                "_id": "tes1@ngdp.com",
                "raw_layer": {"timestamp1": "s3://raw_layer/location1"},
                "standardise_layer": {"timestamp1": "s3://standardise_layer/location1"},
                "ucv_layer": {"timestamp1": "s3://ucv_layer/location1"},
                "quarantine_layer": {"timestamp1": "s3://quarantine/location1"},
                "sandbox": {"timestamp1": "s3://sandbox/location1"},
                "redshift": {
                    "timestamp1": {"schema_name": "model", "table_name": "person"}
                },
            }
        )
    except:
        print("Something else went wrong")

    # This will fail because of DuplicateKeyError:.
    # client[DBNAME][COLLECTION_NAME].insert_one(
    #     {
    #         "_id": "tes1@ngdp.com",
    #         "landing_lay": {"timestamp1": "s3://landing_lay/location1"},
    #         "raw_layer": {"timestamp1": "s3://raw_layer/location1"},
    #         "standardise_layer": {"timestamp1": "s3://standardise_layer/location1"},
    #         "ucv_layer": {"timestamp1": "s3://ucv_layer/location1"},
    #         "quarantine_layer": {"timestamp1": "s3://quarantine/location1"},
    #         "sandbox": {"timestamp1": "s3://sandbox/location1"},
    #         "redshift": {
    #             "timestamp1": {"schema_name": "model", "table_name": "person"}
    #         },
    #     }
    # )

    client[DBNAME][COLLECTION_NAME].update(
        {"_id": "tes1@ngdp.com"},
        {"$set": {"tokenisation_layer": {"timestamp1": "s3://quarantine/location1"}}},
    )

    # Inserting more than one raw.
    data = [
        {
            "_id": "tes2@ngdp.com",
            "raw_layer": {"timestamp1": "s3://raw_layer/location1/"},
            "standardise_layer": {"timestamp1": "s3://standardise_layer/location1"},
            "ucv_layer": {"timestamp1": "s3://ucv_layer/location1"},
            "quarantine_layer": {"timestamp1": "s3://quarantine/location1"},
            "sandbox": {"timestamp1": "s3://sandbox/location1"},
            "redshift": {
                "timestamp1": {"schema_name": "model", "table_name": "person"}
            },
        },
        {
            "_id": "tes3@ngdp.com",
            "raw_layer": {"timestamp1": "s3://raw_layer/location1"},
            "standardise_layer": {"timestamp1": "s3://standardise_layer/location1"},
            "ucv_layer": {"timestamp1": "s3://ucv_layer/location1"},
            "quarantine_layer": {"timestamp1": "s3://quarantine/location1"},
            "sandbox": {"timestamp1": "s3://sandbox/location1"},
            "redshift": {
                "timestamp1": {"schema_name": "model", "table_name": "person"}
            },
        },
    ]
    try:
        client[DBNAME][COLLECTION_NAME].insert_many(data)
    except:
        print("Something else went wrong")

    # Write Dataframe into Mongo.
    # try:
    #     df = pd.read_csv("./mongo-data-access/data/sales.csv")
    # except FileNotFoundError as e:
    #     print(e)

    # client["NGDP"]["Sales"].insert_many(df.to_dict("record"), ordered=False)

    # Update if Record Exits (Chnge one attribut).
    try:
        find_customer_record = list(
            client[DBNAME]["ngpd_shadow_access_testing"].find({"_id": "tes3@ngdp.com"})
        )
        print(find_customer_record)
    except:
        print("Something else went wrong")

    raw_layer_record = {"timestamp3": "s3://raw_layer/location3"}
    raw_layer_record.update(find_customer_record[0]["raw_layer"])

    client[DBNAME][COLLECTION_NAME].find_one_and_update(
        {"_id": "tes3@ngdp.com"}, {"$set": {"raw_layer": raw_layer_record}}
    )

    # Read data .
    i = 0
    for x in client[DBNAME][COLLECTION_NAME].find({}):
        if i < 3:
            print(x)
        else:
            break
        i += 1

    del i

    # for x in client[DBNAME][COLLECTION_NAME].find({"raw_layer": []}):
    #     print(x)

    # for x in client[DBNAME][COLLECTION_NAME].find({"$or": [{"raw_layer": {}}]}):
    #     print(x)

    # Deleteing
    # try:
    #     client[DBNAME][COLLECTION_NAME].delete_one({"_id": "tes2@ngdp.com"})
    # except:
    #     print("Something else went wrong")
    # client[DBNAME][COLLECTION_NAME].delete_one({})


if __name__ == "__main__":
    main()
