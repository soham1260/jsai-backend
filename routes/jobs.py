from flask import Blueprint, request
from utils.db import db

jobs_bp = Blueprint("jobs", __name__)

@jobs_bp.route("/all", methods=["GET"])
def get_all_jobs():
    jobs = list(db["Jobs"].find({}, {"_id": 0}))
    return {"jobs": jobs}

@jobs_bp.route("/<job_id>", methods=["GET"])
def get_job(job_id):
    job = db["Jobs"].find_one({"job_id": job_id}, {"_id": 0})
    return {"job": job}

@jobs_bp.route("/search", methods=["GET"])
def search_jobs():
    query = request.args.get("q", "")
    results = list(db["Jobs"].find(
        {"designation_clean": {"$regex": query, "$options": "i"}},
        {"_id": 0}
    ))
    return {"results": results}
