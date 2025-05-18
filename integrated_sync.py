from pymongo import MongoClient, WriteConcern, ReadPreference
from influxdb_client import InfluxDBClient, Point, WritePrecision
from neo4j import GraphDatabase
from cassandra.cluster import Cluster, ExecutionProfile
from cassandra import ConsistencyLevel
from datetime import datetime, timezone
import redis
import random
import time

# --- MongoDB Setup ---
mongo_uri = "mongodb://localhost:27017"
client = MongoClient(
    mongo_uri,
    w="majority",
    wtimeoutMS=5000,
    read_preference=ReadPreference.PRIMARY
)
mongo_db = client.get_database("HealthcareSystem")
patients_collection = mongo_db.get_collection("patients", write_concern=WriteConcern("majority"))

# --- InfluxDB Setup ---
influx_token = "mysecrettoken"
influx_org = "healthcare"
influx_bucket = "patient_measurements"
influx_client = InfluxDBClient(url="http://localhost:8086", token=influx_token, org=influx_org)
write_api = influx_client.write_api()

# --- Neo4j Setup ---
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "neo4j123"
graph_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

# --- Redis Setup ---
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# --- Cassandra Setup ---
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

# --- MongoDB CRUD ---
def mongo_create_or_update_patient(patient):
    result = patients_collection.update_one(
        {'_id': patient['_id']},
        {'$set': patient},
        upsert=True
    )
    if result.upserted_id:
        print(f"Inserted new patient with _id: {result.upserted_id}")
    else:
        print(f"Updated existing patient with _id: {patient['_id']}")

def mongo_read_patients():
    return list(patients_collection.find())

def mongo_update_patient(patient_id, update_data):
    result = patients_collection.update_one({'_id': patient_id}, {'$set': update_data})
    print(f"Updated {result.modified_count} patient(s)")

def mongo_delete_patient(patient_id):
    result = patients_collection.delete_one({'_id': patient_id})
    if result.deleted_count:
        print(f"Deleted patient with _id: {patient_id}")
    else:
        print(f"No patient found with _id: {patient_id}")

# --- InfluxDB CRUD (Write only) ---
def influx_write_heartbeat(patient_id, value):
    point = Point("heartbeat") \
        .tag("patient_id", patient_id) \
        .field("value", float(value)) \
        .time(datetime.now(timezone.utc), WritePrecision.S)
    write_api.write(bucket=influx_bucket, org=influx_org, record=point)
    print(f"Wrote heartbeat {value:.2f} for {patient_id} to InfluxDB")

# --- Neo4j CRUD ---
def neo4j_create_doctor(doctor_id, name):
    with graph_driver.session() as session:
        session.run("MERGE (d:Doctor {id: $id}) ON CREATE SET d.name = $name", id=doctor_id, name=name)
        print(f"Doctor {name} created/merged.")

def neo4j_create_patient(patient_id, name):
    with graph_driver.session() as session:
        session.run("MERGE (p:Patient {id: $id}) ON CREATE SET p.name = $name", id=patient_id, name=name)
        print(f"Patient {name} created/merged.")

def neo4j_create_treats_relation(doctor_id, patient_id):
    with graph_driver.session() as session:
        session.run("""
            MATCH (d:Doctor {id: $doctor_id}), (p:Patient {id: $patient_id})
            MERGE (d)-[:TREATS]->(p)
        """, doctor_id=doctor_id, patient_id=patient_id)
        print(f"Created TREATS relation between Doctor {doctor_id} and Patient {patient_id}")

# --- Redis CRUD ---
def redis_set_alert(patient_id, message):
    key = f"alert:{patient_id}"
    redis_client.set(key, message)
    print(f"Alert set for {patient_id}")

def redis_get_alert(patient_id):
    key = f"alert:{patient_id}"
    alert = redis_client.get(key)
    print(f"Alert for {patient_id}: {alert.decode() if alert else None}")
    return alert

def redis_clear_alert(patient_id):
    key = f"alert:{patient_id}"
    redis_client.delete(key)
    print(f"Alert cleared for {patient_id}")

# --- Cassandra CRUD ---
def cassandra_insert_analytics(patient_id, metric, value):
    now = datetime.utcnow()
    cassandra_session.execute("""
        INSERT INTO patient_analytics (patient_id, measurement_time, metric, value)
        VALUES (%s, %s, %s, %s)
    """, (patient_id, now, metric, float(value)))
    print(f"Inserted analytics data for {patient_id}")

def cassandra_read_analytics(patient_id):
    rows = cassandra_session.execute("""
        SELECT * FROM patient_analytics WHERE patient_id=%s LIMIT 10
    """, (patient_id,))
    for row in rows:
        print(f"Cassandra Analytics - Patient: {row.patient_id}, Metric: {row.metric}, Value: {row.value}, Time: {row.measurement_time}")

# --- Main function with usage example ---
def main():
    # MongoDB CRUD
    mongo_create_or_update_patient({'_id': 'patient001', 'name': 'Mohamed', 'region': 'North', 'medical_history': ['asthma']})
    mongo_create_or_update_patient({'_id': 'patient002', 'name': 'Fatima', 'region': 'South', 'medical_history': ['hypertension']})
    
    patients = mongo_read_patients()
    print(f"MongoDB Patients: {patients}")
    
    mongo_update_patient('patient001', {'name': 'Mohamed Updated'})
    
    mongo_delete_patient('patient999')  # No-op example
    
    # InfluxDB write
    influx_write_heartbeat('patient001', random.uniform(60, 100))
    
    # Neo4j CRUD
    neo4j_create_doctor('doc001', 'Dr. Alice')
    neo4j_create_patient('patient001', 'Mohamed Updated')
    neo4j_create_treats_relation('doc001', 'patient001')
    
    # Redis CRUD
    redis_set_alert('patient001', 'High heartbeat detected')
    redis_get_alert('patient001')
    redis_clear_alert('patient001')
    
    # Cassandra CRUD
    cassandra_insert_analytics('patient001', 'heartbeat', random.uniform(60, 100))
    cassandra_read_analytics('patient001')
    
    # Close connections
    graph_driver.close()
    influx_client.close()
    cassandra_cluster.shutdown()
    client.close()

if __name__ == "__main__":
    main()
