from dotenv import load_dotenv
import os
from pymongo import MongoClient

load_dotenv()  # loads .env

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
