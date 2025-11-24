from flask import Flask, request
from google.cloud import storage, pubsub_v1
import csv
import base64
import json

app = Flask(__name__)

# Pub/Sub publisher for sending validator results
publisher = pubsub_v1.PublisherClient()
TOPIC = "projects/FA25-I535-bsokolsk-CrsProject/topics/validator-results"

# Buckets
INGEST_BUCKET = "bsokolsk-ingest-bucket"
CLEAN_BUCKET = "bsokolsk-clean-bucket"
ERROR_BUCKET = "bsokolsk-error-bucket"

def validate_file(blob_name):
    """Check if CSV file has required headers"""
    client = storage.Client()
    blob = client.bucket(INGEST_BUCKET).get_blob(blob_name)
    content = blob.download_as_text().splitlines()
    reader = csv.DictReader(content)
    required_headers = ["id", "name", "amount"]
    return all(h in reader.fieldnames for h in required_headers)

@app.post("/")
def validator():
    """Receive Pub/Sub message, validate file, move to clean/error bucket, publish result"""
    envelope = request.get_json()
    if not envelope:
        return "No Pub/Sub message received", 400

    pubsub_message = envelope.get("message")
    if not pubsub_message:
        return "No Pub/Sub message in envelope", 400

    # Decode the base64 message
    data = pubsub_message.get("data")
    if not data:
        return "No data in message", 400

    msg = json.loads(base64.b64decode(data).decode("utf-8"))

    # Safely get file name
    file_name = msg.get("name")
    if not file_name:
        return "No file name in message", 400

    client = storage.Client()
    source_blob = client.bucket(INGEST_BUCKET).blob(file_name)

    if validate_file(file_name):
        # Move to clean bucket
        client.bucket(CLEAN_BUCKET).copy_blob(source_blob, client.bucket(CLEAN_BUCKET))
        source_blob.delete()
        publisher.publish(TOPIC, f"File validated: {file_name}".encode("utf-8"))
        print(f"File validated and moved: {file_name}")
    else:
        # Move to error bucket
        client.bucket(ERROR_BUCKET).copy_blob(source_blob, client.bucket(ERROR_BUCKET))
        source_blob.delete()
        publisher.publish(TOPIC, f"Validation failed: {file_name}".encode("utf-8"))
        print(f"Validation failed, moved to error bucket: {file_name}")

    return "OK", 200

