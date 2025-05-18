from pymongo import MongoClient, WriteConcern, ReadConcern
from pymongo.read_preferences import ReadPreference
from influxdb_client import InfluxDBClient, Point, WritePrecision
from neo4j import GraphDatabase
from cassandra.cluster import Cluster, ExecutionProfile
from cassandra import ConsistencyLevel
from datetime import datetime, timezone
import time
import random
import redis

# اتصال MongoDB مع writeConcern و readConcern للاتساق القوي
mongo_uri = "mongodb://localhost:27017"
client = MongoClient(
    mongo_uri,
    w="majority",             # Write concern: تأكيد الكتابة من أغلبية العقد
    wtimeout=5000,            # مهلة الانتظار 5 ثواني
    readConcernLevel="majority",  # Read concern: قراءة البيانات المؤكدة من أغلبية العقد
    read_preference=ReadPreference.PRIMARY  # قراءة من العقدة الرئيسية لضمان الاتساق القوي
)

mongo_db = client.get_database("HealthcareSystem")
patients_collection = mongo_db.get_collection("patients", write_concern=WriteConcern("majority"))

# إعدادات اتصال InfluxDB
influx_token = "mysecrettoken"
influx_org = "healthcare"
influx_bucket = "patient_measurements"
influx_client = InfluxDBClient(url="http://localhost:8086", token=influx_token, org=influx_org)
write_api = influx_client.write_api()

# اتصال Neo4j
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "neo4j123"

# اتصال Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# إعداد Cassandra مع execution profile يدعم Consistency Level
profile = ExecutionProfile(consistency_level=ConsistencyLevel.ONE)  # اتساق نهائي
cassandra_cluster = Cluster(['127.0.0.1'], port=9042, execution_profiles={'default': profile})
cassandra_session = cassandra_cluster.connect()

# إنشاء keyspace و table في Cassandra (مرة واحدة فقط)
cassandra_session.execute("""
    CREATE KEYSPACE IF NOT EXISTS healthcare
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")
cassandra_session.set_keyspace('healthcare')
cassandra_session.execute("""
    CREATE TABLE IF NOT EXISTS patient_analytics (
        patient_id text,
        measurement_time timestamp,
        metric text,
        value double,
        PRIMARY KEY ((patient_id), measurement_time, metric)
    ) WITH CLUSTERING ORDER BY (measurement_time DESC)
""")

class HealthcareGraph:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    def close(self):
        self.driver.close()
    def create_doctor(self, doctor_id, name):
        with self.driver.session() as session:
            session.run("""
                MERGE (d:Doctor {id: $doctor_id})
                ON CREATE SET d.name = $name
            """, doctor_id=doctor_id, name=name)
    def create_patient(self, patient_id, name):
        with self.driver.session() as session:
            session.run("""
                MERGE (p:Patient {id: $patient_id})
                ON CREATE SET p.name = $name
            """, patient_id=patient_id, name=name)
    def create_treats_relationship(self, doctor_id, patient_id):
        with self.driver.session() as session:
            session.run("""
                MATCH (d:Doctor {id: $doctor_id}), (p:Patient {id: $patient_id})
                MERGE (d)-[:TREATS]->(p)
            """, doctor_id=doctor_id, patient_id=patient_id)

def set_alert(patient_id, message):
    key = f"alert:{patient_id}"
    redis_client.set(key, message)
    print(f"Alert set for {patient_id}: {message}")

def clear_alert(patient_id):
    key = f"alert:{patient_id}"
    redis_client.delete(key)
    print(f"Alert cleared for {patient_id}")

def check_and_alert(patient_id, heartbeat_value):
    if heartbeat_value > 100 or heartbeat_value < 60:
        set_alert(patient_id, f"Abnormal heartbeat detected: {heartbeat_value:.2f}")
    else:
        clear_alert(patient_id)

def write_heartbeat(write_api, patient_id, value):
    point = Point("heartbeat") \
        .tag("patient_id", patient_id) \
        .field("value", float(value)) \
        .time(datetime.now(timezone.utc), WritePrecision.S)
    write_api.write(bucket=influx_bucket, org=influx_org, record=point)
    print(f"Heartbeat {value:.2f} for {patient_id} written to InfluxDB")

def write_cassandra(patient_id, metric, value):
    now = datetime.utcnow()
    cassandra_session.execute("""
        INSERT INTO patient_analytics (patient_id, measurement_time, metric, value)
        VALUES (%s, %s, %s, %s)
    """, (patient_id, now, metric, float(value)))
    print(f"Analytics data for {patient_id} metric {metric} stored in Cassandra.")

def get_patients_by_region(region):
    return patients_collection.find({"region": region})

def main():
    graph = HealthcareGraph(neo4j_uri, neo4j_user, neo4j_password)
    doctor_id = "doc001"
    doctor_name = "Dr. Alice"
    graph.create_doctor(doctor_id, doctor_name)

    regions = ["North", "South"]

    for region in regions:
        print(f"\nPatients in region '{region}':")
        patients = get_patients_by_region(region)
        for patient in patients:
            pid = patient["_id"]
            pname = patient.get("name", "Unknown")

            print(f"- {pid}: {pname}")

            graph.create_patient(pid, pname)
            graph.create_treats_relationship(doctor_id, pid)

            heartbeat_value = random.uniform(50.0, 110.0)
            write_heartbeat(write_api, pid, heartbeat_value)
            write_cassandra(pid, 'heartbeat', heartbeat_value)
            check_and_alert(pid, heartbeat_value)

            time.sleep(1)

    graph.close()
    influx_client.close()
    client.close()
    cassandra_session.shutdown()

if __name__ == "__main__":
    main()
