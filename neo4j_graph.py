from neo4j import GraphDatabase

# إعداد اتصال Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "neo4j123"

class HealthcareGraph:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_doctor(self, doctor_id, name):
        with self.driver.session() as session:
            session.run(
                """
                MERGE (d:Doctor {id: $doctor_id})
                ON CREATE SET d.name = $name
                """,
                doctor_id=doctor_id, name=name)
            print(f"Doctor {name} created or exists.")

    def create_patient(self, patient_id, name):
        with self.driver.session() as session:
            session.run(
                """
                MERGE (p:Patient {id: $patient_id})
                ON CREATE SET p.name = $name
                """,
                patient_id=patient_id, name=name)
            print(f"Patient {name} created or exists.")

    def create_treats_relationship(self, doctor_id, patient_id):
        with self.driver.session() as session:
            session.run(
                """
                MATCH (d:Doctor {id: $doctor_id}), (p:Patient {id: $patient_id})
                MERGE (d)-[:TREATS]->(p)
                """,
                doctor_id=doctor_id, patient_id=patient_id)
            print(f"Relationship TREATS created or exists between Doctor {doctor_id} and Patient {patient_id}.")

    def get_patients_of_doctor(self, doctor_id):
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (d:Doctor {id: $doctor_id})-[:TREATS]->(p:Patient)
                RETURN p.id AS patient_id, p.name AS patient_name
                """,
                doctor_id=doctor_id)
            patients = []
            for record in result:
                patients.append((record["patient_id"], record["patient_name"]))
            return patients

def main():
    graph = HealthcareGraph(uri, user, password)

    # إنشاء بيانات
    graph.create_doctor("doc001", "Dr. Alice")
    graph.create_patient("patient001", "Moha")
    graph.create_patient("patient002", "Sara")

    # إنشاء علاقات
    graph.create_treats_relationship("doc001", "patient001")
    graph.create_treats_relationship("doc001", "patient002")

    # جلب المرضى تحت رعاية الدكتور
    patients = graph.get_patients_of_doctor("doc001")
    print("Patients treated by doc001:")
    for pid, pname in patients:
        print(f"- {pid}: {pname}")

    graph.close()

if __name__ == "__main__":
    main()
