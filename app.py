from flask import Flask
from routes.jobs import jobs_bp
from routes.companies import companies_bp
from routes.insights import insights_bp
from routes.ai import ai_bp
from flask_cors import CORS
from flask import Flask
from dotenv import load_dotenv
load_dotenv()



app = Flask(__name__)
CORS(app)
# Register Blueprints
app.register_blueprint(jobs_bp, url_prefix="/jobs")
app.register_blueprint(companies_bp, url_prefix="/companies")
app.register_blueprint(insights_bp, url_prefix="/insights")
app.register_blueprint(ai_bp, url_prefix="/ai")


@app.route("/")
def home():
    return {"message": "JobScout AI Backend Running"}

if __name__ == "__main__":
    app.run(debug=True)
