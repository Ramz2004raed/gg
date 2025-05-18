import redis

redis_client = redis.Redis(host='localhost', port=6379, db=0)

keys = redis_client.keys('alert:*')
if not keys:
    print("No alerts found.")
else:
    for key in keys:
        alert = redis_client.get(key).decode()
        print(f"{key.decode()}: {alert}")
