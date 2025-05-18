from influxdb_client import InfluxDBClient
from datetime import timedelta

token = "mysecrettoken"
org = "healthcare"
bucket = "patient_measurements"

with InfluxDBClient(url="http://localhost:8086", token=token, org=org) as client:
    query_api = client.query_api()
    query = f'''
    from(bucket:"{bucket}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "heartbeat")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: 5)
    '''
    tables = query_api.query(query)

    for table in tables:
        for record in table.records:
            print(f"Time: {record.get_time()}, Patient ID: {record.values['patient_id']}, Heartbeat: {record.get_value()}")
