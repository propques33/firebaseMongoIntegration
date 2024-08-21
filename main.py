import os
import json
import base64
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, db

# Load the .env file
load_dotenv()

# Decode the base64 encoded Firebase service account JSON and initialize Firebase Admin SDK
try:
    firebase_service_account_base64 = os.getenv('FIREBASE_SERVICE_ACCOUNT_BASE64')
    if not firebase_service_account_base64:
        raise ValueError("FIREBASE_SERVICE_ACCOUNT_BASE64 environment variable is not set")

    firebase_service_account_json = json.loads(base64.b64decode(firebase_service_account_base64).decode('utf-8'))
    cred = credentials.Certificate(firebase_service_account_json)
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://propclean-default-rtdb.firebaseio.com/'
    })
    print("Firebase initialized successfully")
except Exception as e:
    print(f"Failed to initialize Firebase: {e}")
    exit(1)

# MongoDB setup
try:
    mongo_uri = os.getenv('MONGO_URI')
    if not mongo_uri:
        raise ValueError("MONGO_URI environment variable is not set")

    client = MongoClient(mongo_uri)
    db_mongo = client['Propclean']
    print("MongoDB connected successfully")
except Exception as e:
    print(f"Failed to connect to MongoDB: {e}")
    exit(1)

# Fetch data from Firebase and insert into MongoDB
def fetch_and_upload_data():
    try:
        # Fetch data from Firebase
        ref = db.reference('/')
        firebase_data = ref.get()
        print("Data fetched from Firebase")

        # Add today's date to the documents
        today_date = datetime.today().strftime('%Y-%m-%d')

        for key, value in firebase_data.items():
            if key not in ["none", "lastResetDate"]:
                # Add the date to the data
                if isinstance(value, dict):  # Check if the node is a dictionary
                    value['date'] = today_date

                # Insert data into the respective collection in MongoDB
                db_mongo[key].insert_one(value)
                print(f"Inserted data into MongoDB collection: {key}")

        print("Data fetched from Firebase and inserted into MongoDB successfully!")
    except Exception as e:
        print(f"Failed during fetch and upload: {e}")

# Run the data fetch and upload process
fetch_and_upload_data()
