from neo4j import GraphDatabase
import json
import time

# Connect to the Neo4j database
uri = "bolt://localhost:7687"  # Change if using Neo4j Aura or different URI
username = "neo4j"            # Default username
password = "Simple123"    # Change to your Neo4j password
driver = GraphDatabase.driver(uri, auth=(username, password))

def insert_customer(tx, customer):
    query = (
        "CREATE (c:Customer { "
        "customer_id: $customer_id, "
        "name: $name, "
        "email: $email, "
        "city: $city, "
        "signup_date: $signup_date })"
    )
    tx.run(query, customer_id=customer['customer_id'], name=customer['name'], 
           email=customer['email'], city=customer['city'], signup_date=customer['signup_date'])

# Load the sample data from JSON file
with open('sample_customers.json') as json_file:
    customers = json.load(json_file)

start_time = time.perf_counter()

# Insert data into Neo4j
with driver.session() as session:
    for customer in customers:
        session.write_transaction(insert_customer, customer)

end_time = time.perf_counter()

print(f"\n✅ Inserted {len(customers)} records into Neo4j.")
print(f"⏱️ Time taken: {end_time - start_time:.6f} seconds") # 9.894041

print("Data insertion completed.")
