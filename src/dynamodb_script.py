import boto3
import json
import time

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')  # change region if needed
table = dynamodb.Table('CustomerData')

# Load customer data
with open("sample_customers.json") as f:
    customers = json.load(f)

# Optional: Rename customer_id to be the DynamoDB primary key (already is)
# Insert with batch_writer and measure time
start_time = time.perf_counter()

with table.batch_writer(overwrite_by_pkeys=['customer_id']) as batch:
    for customer in customers:
        batch.put_item(Item=customer)

end_time = time.perf_counter()

print(f"\n✅ Inserted {len(customers)} records into DynamoDB.")
print(f"⏱️ Time taken: {end_time - start_time:.4f} seconds") # 30.4528
