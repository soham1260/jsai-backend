import sys, os

# Add project root to import path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

# py -m pipeline.generate_insights

import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime
from math import log
import re

import sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime
from math import log
import re
from dotenv import load_dotenv

# Load .env
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

CLEANED = "Cleaned_Data"
INSIGHTS = "Insights_Daily"


# =========================
#  HELPER FUNCTIONS
# =========================

def extract_seniority(text):
    t = text.lower()
    if "intern" in t:
        return "Intern"
    if "junior" in t or "jr" in t:
        return "Junior"
    if "senior" in t or "sr" in t:
        return "Senior"
    if "lead" in t:
        return "Lead"
    return "Mid-Level"


def extract_skill_keywords(text):
    skills = ["python", "javascript", "java", "react", "aws", "sql",
              "node", "c++", "c#", "docker", "kubernetes", "cloud", "ml", "ai"]
    found = [s for s in skills if s in text.lower()]
    return found if found else ["other"]


def safe_int(x):
    try:
        return int(str(x).replace(",", "").strip())
    except:
        return None


# =========================
#  MAIN INSIGHTS FUNCTION
# =========================

def generate_insights():

    raw = list(db[CLEANED].find({}, {"_id": 0}))
    df = pd.DataFrame(raw)

    if df.empty:
        print("No cleaned data!")
        return

    # Extract seniority + skills
    df["seniority"] = df["designation_clean"].apply(extract_seniority)
    df["skills"] = df["designation_clean"].apply(extract_skill_keywords)
    df["followers_clean"] = df["LinkedIn_Followers"].apply(safe_int)
    df["applicants_clean"] = df["applicants_clean"].fillna(0)

    # Company prestige index
    df["Prestige"] = df.apply(
        lambda r: log((r["followers_clean"] or 1), 10) * (r["employee_max"] if r["employee_max"] != -1 else 1),
        axis=1
    )

    # Flex Index
    df["Flex_Index"] = df.apply(
        lambda r: (r["employee_min"] if r["employee_min"] != -1 else 1) / (r["applicants_clean"] + 1),
        axis=1
    )

    # =======================
    # INSIGHT 1: Industry Heatmap
    # =======================
    industry_counts = df["industry_clean"].value_counts().to_dict()

    # =======================
    # INSIGHT 2: Top Hiring Companies
    # =======================
    top_companies = (
        df.groupby("Name")
        .size()
        .sort_values(ascending=False)
        .head(10)
        .to_dict()
    )

    # =======================
    # INSIGHT 3: Competition Analysis
    # =======================
    med = df["applicants_clean"].median()

    competition = {
        "average_applicants": df["applicants_clean"].mean(),
        "high_competition_jobs": df[df["applicants_clean"] > med]["designation_clean"].tolist()[:15],
        "low_competition_jobs": df[df["applicants_clean"] < med]["designation_clean"].tolist()[:15],
    }

    # =======================
    # INSIGHT 4: Geography
    # =======================
    city_counts = df["city"].value_counts().to_dict()
    state_counts = df["state"].value_counts().to_dict()

    # =======================
    # INSIGHT 5: Seniority Breakdown
    # =======================
    seniority_counts = df["seniority"].value_counts().to_dict()

    # =======================
    # INSIGHT 6: Skills Frequency
    # =======================
    skill_counts = df.explode("skills")["skills"].value_counts().to_dict()

    # =======================
    # INSIGHT 7: Work Type
    # =======================
    worktype_counts = df["work_type_clean"].value_counts().to_dict()

    # =======================
    # INSIGHT 8: Prestige Ranking
    # =======================
    prestige_rank = (
        df.sort_values("Prestige", ascending=False)[["Name", "Prestige"]]
        .head(10)
        .to_dict("records")
    )

    # =======================
    # INSIGHT 9: Flex Index Rankings
    # =======================
    flex_rank = (
        df.sort_values("Flex_Index", ascending=False)[
            ["designation_clean", "Name", "Flex_Index"]
        ]
        .head(10)
        .to_dict("records")
    )

    # =======================
    # INSIGHT 10: Fastest-growing Roles (time-based)
    # =======================
    # Convert to date
    df["date_clean"] = pd.to_datetime(df["date_clean"], errors="coerce")

    role_growth = (
        df.groupby([df["date_clean"].dt.to_period("M"), "designation_clean"])
        .size()
        .unstack(fill_value=0)
        .diff()
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .to_dict()
    )

    # =======================
    # INSIGHT 11: Industry × Seniority Matrix
    # =======================
    industry_seniority = (
        df.groupby(["industry_clean", "seniority"])
        .size()
        .unstack(fill_value=0)
        .to_dict()
    )

    # =======================
    # INSIGHT 12: Best Cities for Each Role
    # =======================
    role_city = (
        df.groupby(["designation_clean", "city"])
        .size()
        .unstack(fill_value=0)
        .to_dict()
    )

    # =======================
    # INSIGHT 13: Avg Company Size per Role
    # =======================
    avg_company_per_role = (
        df.groupby("designation_clean")["employee_min"]
        .mean()
        .sort_values(ascending=False)
        .to_dict()
    )

    # =======================
    # INSIGHT 14: Work-type preference per industry
    # =======================
    industry_worktype = (
        df.groupby(["industry_clean", "work_type_clean"])
        .size()
        .unstack(fill_value=0)
        .to_dict()
    )

    # =======================
    # INSIGHT 15: Industry competitiveness score
    # =======================
    industry_competition_score = (
        df.groupby("industry_clean")["applicants_clean"]
        .median()
        .sort_values(ascending=False)
        .to_dict()
    )

    # =======================
    # INSIGHT 16: City hiring density index
    # =======================
    df["city_hiring_density"] = df.apply(
        lambda r: log(max(1, city_counts.get(r["city"], 1))) *
        (r["employee_min"] if r["employee_min"] != -1 else 1),
        axis=1
    )

    city_density = (
        df.groupby("city")["city_hiring_density"]
        .mean()
        .sort_values(ascending=False)
        .to_dict()
    )

    # =======================
    # INSIGHT 17: Role Popularity Ranking
    # =======================
    role_popularity = (
        df["designation_clean"]
        .value_counts()
        .head(20)
        .to_dict()
    )

    # =======================
    # INSIGHT 18: Hiring Consistency Score
    # =======================
    hiring_consistency = (
        df.groupby("Name")["date_clean"]
        .nunique()
        .sort_values(ascending=False)
        .to_dict()
    )

    # =======================
    # SAVE EVERYTHING
    # =======================
    insights = {
        "timestamp": datetime.utcnow(),
        "industry_heatmap": industry_counts,
        "top_hiring_companies": top_companies,
        "competition": competition,
        "jobs_by_city": city_counts,
        "jobs_by_state": state_counts,
        "seniority_breakdown": seniority_counts,
        "skill_frequency": skill_counts,
        "work_type_distribution": worktype_counts,
        "prestigious_companies": prestige_rank,
        "flex_index_top_roles": flex_rank,

        # NEW INSIGHTS
        "fastest_growing_roles": role_growth,
        "industry_seniority_matrix": industry_seniority,
        "best_cities_for_roles": role_city,
        "avg_company_size_per_role": avg_company_per_role,
        "industry_worktype_preference": industry_worktype,
        "industry_competitiveness_score": industry_competition_score,
        "city_hiring_density": city_density,
        "role_popularity": role_popularity,
        "hiring_consistency_score": hiring_consistency,
    }

    # Write to DB
    db[INSIGHTS].delete_many({})
    db[INSIGHTS].insert_one(insights)

    print("INSIGHTS GENERATED SUCCESSFULLY (ULTRA MODE)")


if __name__ == "__main__":
    generate_insights()
