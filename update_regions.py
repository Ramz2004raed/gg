from pymongo import MongoClient

# اتصال MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["HealthcareSystem"]
patients = db["patients"]

# تحديث مناطق المرضى
def update_patient_regions():
    updates = [
        {"_id": "patient001", "region": "North"},
        {"_id": "patient002", "region": "South"},
        # أضف مرضى آخرين هنا مع مناطقهم
    ]
    for u in updates:
        result = patients.update_one({"_id": u["_id"]}, {"$set": {"region": u["region"]}})
        print(f"Updated {u['_id']}: matched={result.matched_count}, modified={result.modified_count}")

# قراءة المرضى حسب المنطقة
def get_patients_by_region(region):
    results = patients.find({"region": region})
    print(f"\nPatients in region '{region}':")
    for p in results:
        print(f"- ID: {p['_id']}, Name: {p.get('name', 'Unknown')}")

def main():
    update_patient_regions()
    get_patients_by_region("North")
    get_patients_by_region("South")

if __name__ == "__main__":
    main()
    client.close()
