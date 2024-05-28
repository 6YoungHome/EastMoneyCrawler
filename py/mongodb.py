from pymongo import MongoClient
import pandas as pd
import os

class MongoAPI(object):

    def __init__(self, db_name: str, collection_name: str, host='localhost', port=27017):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = MongoClient(host=self.host, port=self.port)
        self.database = self.client[self.db_name]
        self.collection = self.database[self.collection_name]

    def insert_one(self, kv_dict):
        self.collection.insert_one(kv_dict)

    def insert_many(self, li_dict):  # more efficient
        self.collection.insert_many(li_dict)

    def find_one(self, query1, query2):
        return self.collection.find_one(query1, query2)

    def find(self, query1, query2):
        return self.collection.find(query1, query2)

    def find_first(self):
        return self.collection.find_one(sort=[('_id', 1)])
    
    def find_last(self):
        return self.collection.find_one(sort=[('_id', -1)])

    def count_documents(self):
        return self.collection.count_documents({})

    def update_one(self, kv_dict):
        self.collection.update_one(kv_dict, {'$set': kv_dict}, upsert=True)

    def drop(self):
        self.collection.drop()
        
    def save_parquet(self):
        d = self.collection.find({})
        if not os.path.exists(f"{self.db_name}"):
            os.mkdir(f"{self.db_name}")
        df = pd.DataFrame(list(d))
        df.to_parquet(f"{self.db_name}/{self.collection_name}.parquet", index=False)
