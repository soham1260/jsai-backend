from flask import Blueprint
from utils.db import db

companies_bp = Blueprint("companies", __name__)

@companies_bp.route("/all", methods=["GET"])
def get_all_companies():
    companies = list(db["Company"].find({}, {"_id": 0}))
    return {"companies": companies}

@companies_bp.route("/<company_id>", methods=["GET"])
def get_company(company_id):
    company = db["Company"].find_one({"company_id": company_id}, {"_id": 0})
    return {"company": company}

@companies_bp.route("/jobs/<company_id>", methods=["GET"])
def get_jobs_by_company(company_id):
    jobs = list(db["Jobs"].find({"company_id": company_id}, {"_id": 0}))
    return {"jobs": jobs}
