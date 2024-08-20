import os
import json
import base64
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, db
from apscheduler.schedulers.blocking import BlockingScheduler

# Load the .env file
load_dotenv()

# Decode the base64 encoded Firebase service account JSON and initialize Firebase Admin SDK
firebase_service_account_base64 = os.getenv('FIREBASE_SERVICE_ACCOUNT_BASE64')
firebase_service_account_json = json.loads(base64.b64decode(firebase_service_account_base64).decode('utf-8'))
cred = credentials.Certificate(firebase_service_account_json)
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://propclean-default-rtdb.firebaseio.com/'
})

# MongoDB setup
mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
db_mongo = client['Propclean']

# Fetch data from Firebase and insert into MongoDB
def fetch_and_upload_data():
    # Fetch data from Firebase
    ref = db.reference('/')
    firebase_data = ref.get()

    # Add today's date to the documents
    today_date = datetime.today().strftime('%Y-%m-%d')

    for key, value in firebase_data.items():
        if key not in ["none", "lastResetDate"]:
            # Add the date to the data
            if isinstance(value, dict):  # Check if the node is a dictionary
                value['date'] = today_date

            # Insert data into the respective collection in MongoDB
            db_mongo[key].insert_one(value)

    print("Data fetched from Firebase and inserted into MongoDB successfully!")

# Scheduler setup
scheduler = BlockingScheduler()

# Schedule the job to run daily at 11:55 PM
scheduler.add_job(fetch_and_upload_data, 'cron', hour=23, minute=55)

# Start the scheduler
print("Scheduler started. Waiting for the next scheduled job...")
scheduler.start()
