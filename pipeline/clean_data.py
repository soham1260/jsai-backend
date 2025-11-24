import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
import re
import hashlib
from pymongo import MongoClient

# ======================================================
#  MONGO CONNECTION
# ======================================================
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
import re
import hashlib
from pymongo import MongoClient
from dotenv import load_dotenv

# Load .env
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

RAW_COLLECTION = "DailyData"

COMPANY_COLLECTION = "Company"
JOBS_COLLECTION = "Jobs"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# ======================================================
#  UTILITY
# ======================================================
def hash_id(*values):
    text = "|".join([str(v).strip().lower() for v in values])
    return hashlib.md5(text.encode()).hexdigest()

# ======================================================
#  CLEANING FUNCTIONS
# ======================================================
def clean_total_applicants(val):
    if pd.isna(val):
        return -1
    m = re.search(r"\d+", str(val))
    return int(m.group()) if m else -1


def parse_employee_count(val):
    if pd.isna(val):
        return pd.Series([None, None])

    val = val.replace(",", "")
    nums = re.findall(r'\d+', val)

    if "+" in val:  # ex: "10,001+ employees"
        return pd.Series([int(nums[0]), None])

    if len(nums) == 2:
        return pd.Series([int(nums[0]), int(nums[1])])

    if len(nums) == 1:
        n = int(nums[0])
        return pd.Series([n, n])

    return pd.Series([None, None])


def split_location(val):
    if pd.isna(val):
        return pd.Series([None, None, None])

    parts = [p.strip() for p in str(val).split(",")]

    if len(parts) == 1:
        return pd.Series([None, None, parts[0]])

    if len(parts) == 2:
        return pd.Series([None, parts[0], parts[1]])

    # 3 or more parts: city, state, country
    return pd.Series([parts[0], parts[1], parts[-1]])


def clean_work_type(val):
    if pd.isna(val):
        return "Unknown"
    v = val.lower().strip()

    if "on-site" in v or "onsite" in v or "on site" in v:
        return "On-site"
    if "remote" in v:
        return "Remote"
    if "hybrid" in v:
        return "Hybrid"
    return "Unknown"


def normalize_industry(val):
    if pd.isna(val):
        return "Unknown"
    v = val.lower().strip()

    if "software" in v: return "Software Development"
    if "it services" in v or "consulting" in v: return "IT Services"
    if "information technology" in v: return "IT Services"
    if "internet" in v or "publishing" in v: return "Internet / Media"
    if "computer" in v: return "Computer / Cybersecurity"
    if "financial" in v or "bank" in v: return "Financial Services"
    if "insurance" in v: return "Insurance"
    if "semiconductor" in v or "electronics" in v: return "Semiconductor / Hardware"
    if "education" in v or "e-learning" in v: return "Education"
    if "human resources" in v or "recruit" in v or "staffing" in v: return "HR / Staffing"
    if "health" in v or "hospital" in v: return "Healthcare"
    if "manufactur" in v: return "Manufacturing"
    if "logistics" in v or "transport" in v: return "Transportation"
    if "energy" in v or "oil" in v or "gas" in v: return "Energy"

    return "Other"


def clean_followers(val):
    if pd.isna(val):
        return None
    nums = re.findall(r'\d+', str(val))
    return int("".join(nums)) if nums else None


# ======================================================
#  MAIN PIPELINE (2 COLLECTIONS)
# ======================================================
def clean_and_store():

    # Load raw data
    raw_docs = list(db[RAW_COLLECTION].find())
    if not raw_docs:
        print("⚠ No raw data found.")
        return

    df = pd.DataFrame(raw_docs)
    print(f"📥 Loaded {len(df)} raw entries.")

    # -----------------------------------------------------
    # Apply cleaning (your exact transformations)
    # -----------------------------------------------------
    df["applicants_clean"] = df["Total_applicants"].apply(clean_total_applicants)
    df["designation_clean"] = df["Designation"].str.title()
    df[["employee_min", "employee_max"]] = df["Employee_count"].apply(parse_employee_count)
    df["followers_clean"] = df["LinkedIn_Followers"].apply(clean_followers)
    df[["city", "state", "country"]] = df["Location"].apply(split_location)
    df["work_type_clean"] = df["work_type"].apply(clean_work_type)
    df["industry_clean"] = df["Industry"].apply(normalize_industry)
    df["date_clean"] = pd.to_datetime(df["date"], errors="coerce")

    # -----------------------------------------------------
    # Generate IDs
    # -----------------------------------------------------
    df["company_id"] = df.apply(lambda r: hash_id(r["Name"]), axis=1)

    df["job_id"] = df.apply(lambda r: hash_id(
        r["designation_clean"], r["Name"], r["city"], r["state"], r["country"], r["date_clean"]
    ), axis=1)

    # -----------------------------------------------------
    # SPLIT INTO TWO COLLECTIONS
    # -----------------------------------------------------

    # 1️⃣ Company Collection
    company_df = df[[
        "company_id", "Name", "industry_clean",
        "employee_min", "employee_max", "followers_clean"
    ]].drop_duplicates("company_id")

    # 2️⃣ Jobs Collection
    jobs_df = df[[
        "job_id", "designation_clean",
        "employment_type", "work_type_clean",
        "city", "state", "country",
        "applicants_clean",
        "date_clean",
        "company_id"
    ]]

    # -----------------------------------------------------
    # Upload to MongoDB
    # -----------------------------------------------------
    db[COMPANY_COLLECTION].delete_many({})
    db[JOBS_COLLECTION].delete_many({})

    db[COMPANY_COLLECTION].insert_many(company_df.to_dict("records"))
    db[JOBS_COLLECTION].insert_many(jobs_df.to_dict("records"))

    print("✅ CLEANING + NORMALIZATION COMPLETE!")
    print(f"🏢 Companies inserted: {len(company_df)}")
    print(f"💼 Jobs inserted: {len(jobs_df)}")


if __name__ == "__main__":
    clean_and_store()
