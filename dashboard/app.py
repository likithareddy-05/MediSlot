"""Small Flask host for the single-file MediSlot doctor dashboard.

Run from the repository root after installing dashboard/requirements.txt:
    flask --app dashboard.app run --host 0.0.0.0 --port 5000
In production, run behind the existing EC2 reverse proxy / WSGI server.
"""
from pathlib import Path
from flask import Flask, redirect, send_from_directory

app = Flask(__name__)
DASHBOARD_DIR = Path(__file__).resolve().parent


@app.get("/")
def home():
    return redirect("/doctor/dashboard")


@app.get("/doctor/dashboard")
def doctor_dashboard():
    return send_from_directory(DASHBOARD_DIR, "doctor_dashboard.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
