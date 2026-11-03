"""MediSlot doctor dashboard and same-origin backend-for-frontend."""
import logging
import os
import secrets
from pathlib import Path
from urllib.parse import quote

import requests
from flask import Flask, jsonify, redirect, request, send_from_directory, session
from flask_session import Session

app = Flask(__name__)
DASHBOARD_DIR = Path(__file__).resolve().parent
API_BASE_URL = os.environ.get("MEDISLOT_API_BASE_URL", "").rstrip("/")
if not API_BASE_URL:
    raise RuntimeError("MEDISLOT_API_BASE_URL must be configured on the dashboard host")

app.config.update(
    SECRET_KEY=os.environ.get("FLASK_SECRET_KEY") or secrets.token_urlsafe(48),
    SESSION_TYPE="filesystem",
    SESSION_FILE_DIR=os.environ.get("SESSION_FILE_DIR", "/var/lib/medislot/sessions"),
    SESSION_PERMANENT=True,
    PERMANENT_SESSION_LIFETIME=604800,
    SESSION_USE_SIGNER=True,
    SESSION_COOKIE_NAME="medislot_session",
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE", "true").lower() == "true",
)
Session(app)

# Flask's logger defaults to WARNING under gunicorn, which would drop every INFO line.
app.logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO))

HTTP = requests.Session()
TIMEOUT = (5, 25)


def _json_response(response):
    try:
        payload = response.json()
    except ValueError:
        payload = {"message": "The backend returned an invalid response"}
        app.logger.error("upstream_invalid_json status=%s", response.status_code)
    if response.status_code >= 400:
        message = payload.get("message") if isinstance(payload, dict) else None
        app.logger.warning("upstream_error status=%s message=%s", response.status_code, str(message)[:200])
    return jsonify(payload), response.status_code


def _csrf_valid():
    supplied = request.headers.get("X-CSRF-Token", "")
    expected = session.get("csrf_token", "")
    return bool(supplied and expected and secrets.compare_digest(supplied, expected))


def _api_call(path, method="GET", *, token=None, body=None, params=None):
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        headers["Content-Type"] = "application/json"
    return HTTP.request(
        method, f"{API_BASE_URL}/{quote(path.lstrip('/'), safe='/')}", headers=headers,
        json=body, params=params, timeout=TIMEOUT,
    )


def _refresh_access_token():
    refresh_token = session.get("refresh_token")
    if not refresh_token:
        return False
    response = _api_call("auth/refresh", "POST", body={"refresh_token": refresh_token})
    if response.status_code != 200:
        app.logger.warning("bff_token_refresh_failed status=%s user_id=%s",
                           response.status_code, (session.get("user") or {}).get("user_id"))
        session.clear()
        return False
    session["access_token"] = response.json()["access_token"]
    app.logger.info("bff_token_refreshed user_id=%s", (session.get("user") or {}).get("user_id"))
    return True


@app.get("/")
def home():
    return redirect("/doctor/dashboard")


@app.get("/doctor/dashboard")
def doctor_dashboard():
    return send_from_directory(DASHBOARD_DIR, "doctor_dashboard.html")


@app.get("/bff/session")
def bff_session():
    session.setdefault("csrf_token", secrets.token_urlsafe(32))
    if not session.get("access_token"):
        return jsonify({"authenticated": False, "csrf_token": session["csrf_token"]})
    return jsonify({"authenticated": True, "csrf_token": session["csrf_token"],
                    "user": session.get("user", {})})


@app.get("/bff/lookups")
def bff_lookups():
    """Hospital and specialization dropdown lists (public reference data, no sign-in needed)."""
    try:
        response = _api_call("lookups")
    except requests.RequestException:
        app.logger.exception("Lookups API request failed")
        return jsonify({"message": "Unable to load the hospital and specialization lists."}), 502
    return _json_response(response)


@app.post("/bff/register")
def bff_register():
    if not _csrf_valid():
        return jsonify({"message": "Your session expired. Refresh the page and try again."}), 403
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"message": "Invalid registration data."}), 400
    payload["role"] = "DOCTOR"
    try:
        response = _api_call("auth/register", "POST", body=payload)
    except requests.RequestException:
        app.logger.exception("Doctor registration API request failed")
        return jsonify({"message": "Unable to reach the registration service. Please try again."}), 502
    return _json_response(response)


@app.post("/bff/login")
def bff_login():
    if not _csrf_valid():
        return jsonify({"message": "Your session expired. Refresh the page and try again."}), 403
    payload = request.get_json(silent=True) or {}
    email, password = payload.get("email"), payload.get("password")
    if not isinstance(email, str) or not isinstance(password, str) or not email.strip() or not password:
        return jsonify({"message": "Email and password are required."}), 400
    try:
        response = _api_call("auth/login", "POST",
                             body={"email": email.strip(), "password": password})
    except requests.RequestException:
        app.logger.exception("Authentication API request failed")
        return jsonify({"message": "Unable to reach the sign-in service. Please try again."}), 502
    if response.status_code != 200:
        app.logger.warning("bff_login_rejected status=%s source_ip=%s", response.status_code, request.remote_addr)
        return _json_response(response)
    data = response.json()
    user = data.get("user") or {}
    if user.get("role") != "DOCTOR":
        app.logger.warning("bff_login_wrong_role user_id=%s role=%s", user.get("user_id"), user.get("role"))
        return jsonify({"message": "This portal is only for doctor accounts."}), 403
    if not data.get("access_token") or not data.get("refresh_token"):
        app.logger.error("bff_login_incomplete_session user_id=%s", user.get("user_id"))
        return jsonify({"message": "The sign-in service returned an incomplete session."}), 502
    session.clear()
    try:
        app.session_interface.regenerate(session)
    except (AttributeError, RuntimeError):
        pass
    session.permanent = True
    session["access_token"] = data["access_token"]
    session["refresh_token"] = data["refresh_token"]
    session["user"] = {"user_id": user.get("user_id"), "email": user.get("email"),
                       "role": user.get("role")}
    session["csrf_token"] = secrets.token_urlsafe(32)
    app.logger.info("bff_login_success user_id=%s", user.get("user_id"))
    return jsonify({"message": "Login successful", "user": session["user"],
                    "csrf_token": session["csrf_token"]})


@app.post("/bff/logout")
def bff_logout():
    if not _csrf_valid():
        return jsonify({"message": "Invalid session request."}), 403
    access_token, refresh_token = session.get("access_token"), session.get("refresh_token")
    app.logger.info("bff_logout user_id=%s", (session.get("user") or {}).get("user_id"))
    if access_token and refresh_token:
        try:
            result = _api_call("auth/logout", "POST", token=access_token,
                               body={"refresh_token": refresh_token})
            if result.status_code == 401 and _refresh_access_token():
                _api_call("auth/logout", "POST", token=session.get("access_token"),
                          body={"refresh_token": refresh_token})
        except requests.RequestException:
            app.logger.exception("Logout API request failed")
    session.clear()
    response = jsonify({"message": "Logged out"})
    response.delete_cookie(app.config["SESSION_COOKIE_NAME"])
    return response


def _local_doctor_path(api_path):
    """Map same-origin dashboard paths to the documented MediSlot API routes."""
    path = "/" + api_path.strip("/")

    # Profile
    if path == "/profile":
        return "doctors/me"
    if path == "/profile/photo":
        return "doctors/me/photo"

    # Doctor-domain collections and item actions
    mappings = (
        ("/consultation-types", "/doctors/me/consultation-types"),
        ("/availability", "/doctors/me/availability"),
        ("/blackouts", "/doctors/me/blackouts"),
        ("/schedule", "/doctors/me/schedule"),
        ("/appointments", "/doctors/me/appointments"),
        ("/slots", "/doctors/me/slots"),
        ("/materialize", "/doctors/me/materialize"),
    )
    for source, target in mappings:
        if path == source or path.startswith(source + "/"):
            return target + path[len(source):]
    return None


@app.route("/bff/api/<path:api_path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def bff_api(api_path):
    if not session.get("access_token"):
        return jsonify({"message": "Please sign in again."}), 401
    if request.method not in {"GET", "HEAD"} and not _csrf_valid():
        return jsonify({"message": "Invalid session request. Refresh the page and try again."}), 403

    upstream_path = _local_doctor_path(api_path)
    if not upstream_path:
        return jsonify({"message": "Dashboard API route is not supported."}), 404
    body = request.get_json(silent=True) if request.method not in {"GET", "DELETE"} else None
    if api_path.strip("/") == "profile/photo" and isinstance(body, dict):
        body.setdefault("filename", "profile.jpg")

    try:
        upstream = _api_call(upstream_path, request.method, token=session.get("access_token"),
                             body=body, params=request.args)
        if upstream.status_code == 401 and _refresh_access_token():
            upstream = _api_call(upstream_path, request.method,
                                 token=session.get("access_token"), body=body,
                                 params=request.args)
    except requests.RequestException:
        app.logger.exception("Doctor API request failed")
        return jsonify({"message": "Unable to reach the MediSlot service. Please try again."}), 502
    if upstream.status_code == 401:
        app.logger.warning("bff_session_expired user_id=%s path=%s",
                           (session.get("user") or {}).get("user_id"), upstream_path)
        session.clear()
    return _json_response(upstream)


@app.after_request
def _log_failed_bff_requests(resp):
    """One WARNING/ERROR line for every failed dashboard API call (4xx/5xx)."""
    if request.path.startswith("/bff/") and resp.status_code >= 400:
        level = logging.ERROR if resp.status_code >= 500 else logging.WARNING
        app.logger.log(level, "bff_request_failed method=%s path=%s status=%s user_id=%s",
                       request.method, request.path, resp.status_code,
                       (session.get("user") or {}).get("user_id"))
    return resp


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
