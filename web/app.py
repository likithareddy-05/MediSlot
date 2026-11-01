import os, requests
from flask import Flask, render_template, request, redirect, session, jsonify, flash
from flask_session import Session

app=Flask(__name__)
app.secret_key=os.environ["FLASK_SECRET_KEY"]
app.config.update(SESSION_TYPE="filesystem", SESSION_FILE_DIR="/var/lib/medislot/sessions",
                  SESSION_PERMANENT=True, SESSION_USE_SIGNER=True,
                  SESSION_COOKIE_HTTPONLY=True,SESSION_COOKIE_SECURE=os.environ.get("COOKIE_SECURE","true").lower()=="true",SESSION_COOKIE_SAMESITE="Lax",PERMANENT_SESSION_LIFETIME=3600)
Session(app)
API_BASE=os.environ["MEDISLOT_API_BASE"].rstrip("/")

def api(method,path,**kwargs):
    headers=kwargs.pop("headers",{})
    if session.get("access_token"): headers["Authorization"]="Bearer "+session["access_token"]
    response=requests.request(method,API_BASE+path,headers=headers,timeout=20,**kwargs)
    # Refresh the short-lived access JWT once, without rotating/extending the
    # fixed-lifetime opaque refresh token stored by the existing Auth Lambda.
    if response.status_code==401 and session.get("refresh_token") and path not in {"/auth/login","/auth/doctors/login","/auth/refresh"}:
        refreshed=requests.post(API_BASE+"/auth/refresh",json={"refresh_token":session["refresh_token"]},timeout=20)
        if refreshed.ok:
            new_token=refreshed.json().get("access_token")
            if new_token:
                session["access_token"]=new_token
                headers["Authorization"]="Bearer "+new_token
                response=requests.request(method,API_BASE+path,headers=headers,timeout=20,**kwargs)
    return response

@app.get("/")
def home():
    return render_template("portal.html",mode="home")

@app.route("/doctor/login",methods=["GET","POST"])
def doctor_login():
    if request.method=="POST":
        r=api("POST","/auth/doctors/login",json={"email":request.form.get("email"),"password":request.form.get("password")})
        data=r.json()
        if r.ok and (data.get("user") or {}).get("role")=="DOCTOR":
            session.clear(); session["access_token"]=data.get("access_token"); session["refresh_token"]=data.get("refresh_token"); session["role"]="DOCTOR"; session.permanent=True
            return redirect("/doctor")
        flash(data.get("message","Unable to sign in as doctor"))
    return render_template("portal.html",mode="login")

@app.route("/patient/login",methods=["GET","POST"])
def patient_login():
    if request.method=="POST":
        r=api("POST","/auth/login",json={"email":request.form.get("email"),"password":request.form.get("password")})
        data=r.json()
        if r.ok and (data.get("user") or {}).get("role")=="PATIENT":
            session.clear(); session["access_token"]=data.get("access_token"); session["refresh_token"]=data.get("refresh_token"); session["role"]="PATIENT"; session.permanent=True
            return redirect("/patient")
        flash(data.get("message","Unable to sign in as patient"))
    return render_template("portal.html",mode="patient_login")

@app.get("/patient")
def patient_portal():
    if session.get("role")!="PATIENT" or not session.get("access_token"): return redirect("/patient/login")
    return render_template("portal.html",mode="patient_portal")

@app.route("/doctor/register",methods=["GET","POST"])
def doctor_register():
    if request.method=="POST":
        payload={k:request.form.get(k) for k in ["email","password","first_name","last_name","phone","date_of_birth","gender","address","specialization","license_number","years_of_experience","clinic_address"]}
        payload["role"]="DOCTOR"
        r=api("POST","/auth/doctors/register",json=payload)
        if r.ok: flash("Registration successful. Please sign in."); return redirect("/doctor/login")
        flash(r.json().get("message","Registration failed"))
    return render_template("portal.html",mode="register")

@app.get("/doctor")
def doctor_portal():
    if session.get("role")!="DOCTOR" or not session.get("access_token"): return redirect("/doctor/login")
    return render_template("portal.html",mode="portal")

@app.route("/doctor/logout",methods=["POST","GET"])
def doctor_logout():
    token=session.get("refresh_token")
    if token:
        try: api("POST",json={"refresh_token":token})
        except requests.RequestException: pass
    session.clear()
    return redirect("/")

@app.route("/doctor/api/<path:subpath>",methods=["GET","POST","PUT","DELETE"])
def doctor_api_proxy(subpath):
    if session.get("role")!="DOCTOR" or not session.get("access_token"): return jsonify({"message":"Unauthenticated"}),401
    path="/"+subpath
    if request.query_string: path+="?"+request.query_string.decode()
    try:
        upstream=api(request.method,path,json=request.get_json(silent=True) if request.method in {"POST","PUT"} else None)
        return (upstream.text,upstream.status_code,{"Content-Type":upstream.headers.get("Content-Type","application/json")})
    except requests.RequestException:
        return jsonify({"message":"MediSlot API is temporarily unavailable"}),502

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.environ.get("PORT","5000")))
