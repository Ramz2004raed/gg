from pymongo import MongoClient, WriteConcern, ReadPreference
from influxdb_client import InfluxDBClient, Point, WritePrecision
from neo4j import GraphDatabase
from cassandra.cluster import Cluster, ExecutionProfile
from cassandra import ConsistencyLevel
from datetime import datetime, timezone
import redis
import random
import time

# --- إعدادات MongoDB ---
mongo_uri = "mongodb://localhost:27017"
client = MongoClient(
    mongo_uri,
    w="majority",
    wtimeoutMS=5000,
    read_preference=ReadPreference.PRIMARY
)
mongo_db = client.get_database("HealthcareSystem")
patients_collection = mongo_db.get_collection("patients", write_concern=WriteConcern("majority"))
doctors_collection = mongo_db.get_collection("doctors", write_concern=WriteConcern("majority"))

# --- إعدادات InfluxDB ---
influx_token = "mysecrettoken"
influx_org = "healthcare"
influx_bucket = "patient_measurements"
influx_client = InfluxDBClient(url="http://localhost:8086", token=influx_token, org=influx_org)
write_api = influx_client.write_api()

# --- إعدادات Neo4j ---
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "neo4j123"
graph_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

# --- إعدادات Redis ---
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# --- إعدادات Cassandra ---
profile = ExecutionProfile(consistency_level=ConsistencyLevel.ONE)
cassandra_cluster = Cluster(['127.0.0.1'], port=9042, execution_profiles={'default': profile})
cassandra_session = cassandra_cluster.connect()

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

# --- وظائف Neo4j ---

def neo4j_create_doctor(doctor_id, name):
    with graph_driver.session() as session:
        session.run("MERGE (d:Doctor {id: $id}) ON CREATE SET d.name = $name", id=doctor_id, name=name)
        print(f"Doctor {name} created/merged in Neo4j.")

def neo4j_create_patient(patient_id, name):
    with graph_driver.session() as session:
        session.run("MERGE (p:Patient {id: $id}) ON CREATE SET p.name = $name", id=patient_id, name=name)
        print(f"Patient {name} created/merged in Neo4j.")

def neo4j_create_treats_relation(doctor_id, patient_id):
    with graph_driver.session() as session:
        session.run("""
            MATCH (d:Doctor {id: $doctor_id}), (p:Patient {id: $patient_id})
            MERGE (d)-[:TREATS]->(p)
        """, doctor_id=doctor_id, patient_id=patient_id)
        print(f"Created TREATS relation between Doctor {doctor_id} and Patient {patient_id} in Neo4j.")

# --- Redis وظائف التنبيهات ---

def redis_set_alert(patient_id, message):
    key = f"alert:{patient_id}"
    redis_client.set(key, message)
    print(f"Alert set for {patient_id} in Redis.")

def redis_clear_alert(patient_id):
    key = f"alert:{patient_id}"
    redis_client.delete(key)
    print(f"Alert cleared for {patient_id} in Redis.")

# --- كتابة البيانات في InfluxDB و Cassandra ---

def influx_write_heartbeat(patient_id, value):
    point = Point("heartbeat") \
        .tag("patient_id", patient_id) \
        .field("value", float(value)) \
        .time(datetime.now(timezone.utc), WritePrecision.S)
    write_api.write(bucket=influx_bucket, org=influx_org, record=point)
    print(f"Wrote heartbeat {value:.2f} for {patient_id} to InfluxDB.")

def cassandra_insert_analytics(patient_id, metric, value):
    now = datetime.utcnow()
    cassandra_session.execute("""
        INSERT INTO patient_analytics (patient_id, measurement_time, metric, value)
        VALUES (%s, %s, %s, %s)
    """, (patient_id, now, metric, float(value)))
    print(f"Inserted analytics data for {patient_id} into Cassandra.")

# --- التحقق من التنبيهات ---

def check_and_alert(patient_id, heartbeat_value):
    if heartbeat_value > 100 or heartbeat_value < 60:
        redis_set_alert(patient_id, f"Abnormal heartbeat detected: {heartbeat_value:.2f}")
    else:
        redis_clear_alert(patient_id)

# --- المعالجة الرئيسية ---

def process_all():
    # أولاً: إنشاء الأطباء في Neo4j بناءً على بيانات MongoDB
    doctors = list(doctors_collection.find())
    for doc in doctors:
        neo4j_create_doctor(doc["_id"], doc.get("name", "Unknown"))

    # ثم: معالجة المرضى
    patients = list(patients_collection.find())
    for patient in patients:
        pid = patient["_id"]
        pname = patient.get("name", "Unknown")
        doctor_id = patient.get("doctor_id")  # ربط المريض بالطبيب في MongoDB

        neo4j_create_patient(pid, pname)
        if doctor_id:
            neo4j_create_treats_relation(doctor_id, pid)

        heartbeat_value = random.uniform(50.0, 110.0)
        influx_write_heartbeat(pid, heartbeat_value)
        cassandra_insert_analytics(pid, 'heartbeat', heartbeat_value)
        check_and_alert(pid, heartbeat_value)

        time.sleep(1)

    graph_driver.close()
    influx_client.close()
    cassandra_cluster.shutdown()
    client.close()

if __name__ == "__main__":
    process_all()
