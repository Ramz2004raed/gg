from pymongo import MongoClient

class MongoDBHandler:
    def __init__(self, uri="mongodb://localhost:27017", db_name="HealthcareSystem"):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.patients_collection = self.db["patients"]

    def get_patients_by_region(self, region):
        return self.patients_collection.find({"region": region})

    def close(self):
        self.client.close()
