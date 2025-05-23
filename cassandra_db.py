from cassandra.cluster import Cluster
from datetime import datetime

class CassandraHandler:
    def __init__(self, hosts=['127.0.0.1'], port=9042):
        self.cluster = Cluster(hosts, port=port)
        self.session = self.cluster.connect()
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS healthcare
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """)
        self.session.set_keyspace('healthcare')
   cassandra_session.execute("""
    CREATE TABLE IF NOT EXISTS patient_analytics (
    patient_id text,
    measurement_time timestamp,
    metric text,
    value double,
    PRIMARY KEY ((patient_id), measurement_time, metric)
     ) WITH CLUSTERING ORDER BY (measurement_time DESC)
""")

    def insert_patient_metric(self, patient_id, metric, value):
        now = datetime.utcnow()
        self.session.execute("""
            INSERT INTO patient_analytics (patient_id, measurement_time, metric, value)
            VALUES (%s, %s, %s, %s)
        """, (patient_id, now, metric, float(value)))

    def close(self):
        self.cluster.shutdown()
