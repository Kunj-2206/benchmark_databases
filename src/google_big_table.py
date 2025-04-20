from google.cloud import bigtable
from google.cloud.bigtable import column_family
import json
import time

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "diesel-talon-457414-c3-fb1adf8db4aa.json"

# Initialize Bigtable client and table
client = bigtable.Client(project='diesel-talon-457414-c3', admin=True)
instance = client.instance('customerdata')  # Replace with your instance name
table = instance.table('CustomerDataTable')

# Load sample customer data
with open('sample_customers.json') as f:
    customers = json.load(f)

# Insert data into Bigtable and measure time
start_time = time.perf_counter()

# Define column family and column names
column_family_id = 'cf'  # Column family id
for customer in customers:
    row_key = customer['customer_id'].encode('utf-8')
    row = table.row(row_key)
    row.set_cell(column_family_id, 'name', customer['name'])
    row.set_cell(column_family_id, 'email', customer['email'])
    row.set_cell(column_family_id, 'city', customer['city'])
    row.set_cell(column_family_id, 'signup_date', customer['signup_date'])
    row.commit()

end_time = time.perf_counter()

print(f"\n✅ Inserted {len(customers)} records into Google Bigtable.")
print(f"⏱️ Time taken: {end_time - start_time:.4f} seconds") #26.3094


# Example: Update row for customer_id = "cust123"
def update_customer():
    print("🔄 Updating customer 'cust123'...")
    row_key = b'cust123'
    row = table.row(row_key)
    row.set_cell(column_family_id, 'city', 'Toronto')
    row.set_cell(column_family_id, 'email', 'updated_email@example.com')
    row.commit()
    print("✅ Customer updated.")

# Example: Delete row for customer_id = "cust123"
def delete_customer():
    print("🗑️ Deleting customer 'cust123'...")
    row_key = b'cust123'
    table.delete_rows(row_key)
    print("✅ Customer deleted.")
