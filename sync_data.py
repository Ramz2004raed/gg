from pymongo import MongoClient
from influxdb_client import InfluxDBClient, Point, WritePrecision
from datetime import datetime, timezone
import time
import random

def write_heartbeat(write_api, patient_id, value):
    point = Point("heartbeat") \
        .tag("patient_id", patient_id) \
        .field("value", float(value)) \
        .time(datetime.now(timezone.utc), WritePrecision.S)
    write_api.write(bucket="patient_measurements", org="healthcare", record=point)
    print(f"Heartbeat {value:.2f} for {patient_id} written to InfluxDB")

def main():
    mongo_client = MongoClient("mongodb://localhost:27017")
    mongo_db = mongo_client["HealthcareSystem"]
    patients_collection = mongo_db["patients"]

    with InfluxDBClient(url="http://localhost:8086", token="mysecrettoken", org="healthcare") as influx_client:
        write_api = influx_client.write_api()

        patients = patients_collection.find()
        for patient in patients:
            pid = patient["_id"]
            heartbeat_value = random.uniform(60.0, 100.0)
            write_heartbeat(write_api, pid, heartbeat_value)
            time.sleep(1)

    mongo_client.close()

if __name__ == "__main__":
    main()
