from flask import Blueprint, jsonify
from pymongo import MongoClient
from flask import Blueprint, jsonify
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

insights_bp = Blueprint("insights", __name__)


INSIGHTS = "Insights_Daily"


def get_latest_insights():
    return db[INSIGHTS].find_one({}, {"_id": 0}, sort=[("timestamp", -1)])


# ========== GENERAL ==========
@insights_bp.route("/all", methods=["GET"])
def get_all_insights():
    return jsonify(get_latest_insights())


# ========== INDIVIDUAL ENDPOINTS ==========

@insights_bp.route("/industry", methods=["GET"])
def industry_heatmap():
    return jsonify(get_latest_insights().get("industry_heatmap"))


@insights_bp.route("/top-companies", methods=["GET"])
def top_companies():
    return jsonify(get_latest_insights().get("top_hiring_companies"))


@insights_bp.route("/competition", methods=["GET"])
def competition():
    return jsonify(get_latest_insights().get("competition"))


@insights_bp.route("/jobs-by-city", methods=["GET"])
def jobs_by_city():
    return jsonify(get_latest_insights().get("jobs_by_city"))


@insights_bp.route("/jobs-by-state", methods=["GET"])
def jobs_by_state():
    return jsonify(get_latest_insights().get("jobs_by_state"))


@insights_bp.route("/seniority", methods=["GET"])
def seniority():
    return jsonify(get_latest_insights().get("seniority_breakdown"))


@insights_bp.route("/skills", methods=["GET"])
def skills():
    return jsonify(get_latest_insights().get("skill_frequency"))


@insights_bp.route("/worktype", methods=["GET"])
def worktype():
    return jsonify(get_latest_insights().get("work_type_distribution"))


@insights_bp.route("/prestige", methods=["GET"])
def prestige():
    return jsonify(get_latest_insights().get("prestigious_companies"))


@insights_bp.route("/flex-index", methods=["GET"])
def flex_index():
    return jsonify(get_latest_insights().get("flex_index_top_roles"))


@insights_bp.route("/role-growth", methods=["GET"])
def role_growth():
    return jsonify(get_latest_insights().get("fastest_growing_roles"))


@insights_bp.route("/industry-seniority", methods=["GET"])
def industry_seniority():
    return jsonify(get_latest_insights().get("industry_seniority_matrix"))


@insights_bp.route("/best-cities-by-role", methods=["GET"])
def best_cities_by_role():
    return jsonify(get_latest_insights().get("best_cities_for_roles"))


@insights_bp.route("/company-size-per-role", methods=["GET"])
def company_size_role():
    return jsonify(get_latest_insights().get("avg_company_size_per_role"))


@insights_bp.route("/industry-worktype", methods=["GET"])
def industry_worktype():
    return jsonify(get_latest_insights().get("industry_worktype_preference"))


@insights_bp.route("/industry-competition", methods=["GET"])
def industry_competition():
    return jsonify(get_latest_insights().get("industry_competitiveness_score"))


@insights_bp.route("/city-density", methods=["GET"])
def city_density():
    return jsonify(get_latest_insights().get("city_hiring_density"))


@insights_bp.route("/role-popularity", methods=["GET"])
def role_popularity():
    return jsonify(get_latest_insights().get("role_popularity"))


@insights_bp.route("/hiring-consistency", methods=["GET"])
def hiring_consistency():
    return jsonify(get_latest_insights().get("hiring_consistency_score"))
