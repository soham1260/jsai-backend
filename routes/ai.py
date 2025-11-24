from flask import Blueprint, request, jsonify
import requests
import google.generativeai as genai
from flask import Blueprint, request, jsonify
import requests
import google.generativeai as genai
from dotenv import load_dotenv
import os

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=GEMINI_API_KEY)

ai_bp = Blueprint("ai", __name__)


# Configure Gemini API
genai.configure(api_key=GEMINI_API_KEY)


def call_internal_api(path: str):
    """
    Calls our OWN backend API internally.
    Example: path="insights/top-companies"
    → GET http://localhost:5000/insights/top-companies
    """
    url = f"http://localhost:5000/{path}"
    try:
        res = requests.get(url)
        return res.json()
    except Exception as e:
        return {"error": f"Internal API call failed: {str(e)}"}


@ai_bp.route("/query", methods=["POST"])
def ai_query():
    data = request.json

    if "path" not in data or "prompt" not in data:
        return jsonify({"error": "path and prompt are required"}), 400

    path = data["path"]
    user_prompt = data["prompt"]

    # 1. Fetch internal API data
    internal_data = call_internal_api(path)

    # 2. Build LLM prompt
    full_prompt = f"""
    You are JobScoutAI Analyst Bot.

    Here is the backend data for route: /{path}

    DATA:
    {internal_data}

    USER PROMPT:
    {user_prompt}

    Now generate a clean response with:
    - insights
    - explanation
    - simplified breakdown (if needed)
    - recommendations (if applicable)
    """

    # 3. Call Gemini
    model = genai.GenerativeModel("gemini-2.5-flash")
    ai_response = model.generate_content(full_prompt)

    # 4. Return JSON
    return jsonify({
        "route": path,
        "ai_response": ai_response.text
    })
