from flask import Flask, request
from google.cloud import storage, pubsub_v1
import csv

app = Flask(__name__)
publisher = pubsub_v1.PublisherClient()
TOPIC = "projects/FA25-I535-bsokolsk-CrsProject/PROJECT/topics/validator-results"

INGEST_BUCKET = "bsokolsk-ingest-bucket"
CLEAN_BUCKET = "bsokolsk-clean-bucket"
ERROR_BUCKET = "bsokolsk-error-bucket"

def validate_file(blob_name):
    client = storage.Client()
    blob = client.bucket(INGEST_BUCKET).get_blob(blob_name)
    content = blob.download_as_text().splitlines()
    reader = csv.DictReader(content)
    required_headers = ["id", "name", "amount"]
    return all(h in reader.fieldnames for h in required_headers)

@app.post("/")
def validator():
    msg = request.get_json()
    file_name = msg["name"]
    client = storage.Client()
    source_blob = client.bucket(INGEST_BUCKET).blob(file_name)

    if validate_file(file_name):
        client.bucket(CLEAN_BUCKET).copy_blob(source_blob, client.bucket(CLEAN_BUCKET))
        source_blob.delete()
        publisher.publish(TOPIC, f"File validated: {file_name}".encode("utf-8"))
    else:
        client.bucket(ERROR_BUCKET).copy_blob(source_blob, client.bucket(ERROR_BUCKET))
        source_blob.delete()
        publisher.publish(TOPIC, f"Validation failed: {file_name}".encode("utf-8"))

    return "OK", 200
