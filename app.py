"""
S. Shalabi — Data Analytics & AI Portfolio
Flask app combining portfolio site + ClearPath tool + admin data upload
"""

import os, re, json, secrets, hashlib
from functools import wraps
from flask import (Flask, render_template, request, jsonify,
                   send_file, redirect, url_for, session, flash)
import duckdb
from anthropic import Anthropic
from werkzeug.utils import secure_filename

app    = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))
client = Anthropic()

BASE    = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE, "providers.db")
XL_PATH = os.path.join(BASE, "providers_database.xlsx")
UPLOAD_FOLDER = os.path.join(BASE, "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Admin password — change this or set via env var
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "clearpath2026")

# ── Auth ─────────────────────────────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return decorated

# ── DuckDB helpers ────────────────────────────────────────────────────────────
def get_db():
    return duckdb.connect(DB_PATH, read_only=True)

def db_exists():
    return os.path.exists(DB_PATH)

def row_to_dict(row):
    if not row: return None
    return {
        "npi": row[0], "last_name": row[1] or "", "first_name": row[2] or "",
        "partb": row[3]=="Y", "dme": row[4]=="Y", "hha": row[5]=="Y",
        "pmd":   row[6]=="Y", "hospice": row[7]=="Y",
    }

def lookup_npi(npi):
    if not db_exists(): return None
    with get_db() as con:
        row = con.execute(
            "SELECT NPI,LAST_NAME,FIRST_NAME,PARTB,DME,HHA,PMD,HOSPICE "
            "FROM providers WHERE NPI=?", [npi.strip()]
        ).fetchone()
    return row_to_dict(row)

def search_name(last, limit=8):
    if not db_exists(): return []
    with get_db() as con:
        rows = con.execute(
            "SELECT NPI,LAST_NAME,FIRST_NAME,PARTB,DME,HHA,PMD,HOSPICE "
            "FROM providers WHERE LAST_NAME LIKE ? LIMIT ?",
            [last.upper().strip()+"%", limit]
        ).fetchall()
    return [row_to_dict(r) for r in rows]

def get_stats():
    if not db_exists(): return {}
    with get_db() as con:
        r = con.execute("""
            SELECT COUNT(*),
              SUM(CASE WHEN PARTB='Y'   THEN 1 ELSE 0 END),
              SUM(CASE WHEN DME='Y'     THEN 1 ELSE 0 END),
              SUM(CASE WHEN HHA='Y'     THEN 1 ELSE 0 END),
              SUM(CASE WHEN PMD='Y'     THEN 1 ELSE 0 END),
              SUM(CASE WHEN HOSPICE='Y' THEN 1 ELSE 0 END),
              SUM(CASE WHEN PARTB='Y' AND DME='Y' AND HHA='Y'
                            AND PMD='Y' AND HOSPICE='Y' THEN 1 ELSE 0 END)
            FROM providers
        """).fetchone()
    return {k: int(v) for k, v in zip(
        ["total","partb","dme","hha","pmd","hospice","all5"], r)}

def build_db_from_file(filepath):
    """Rebuild DuckDB from CSV or Excel file."""
    ext = os.path.splitext(filepath)[1].lower()
    con = duckdb.connect(DB_PATH)
    if ext == ".csv":
        con.execute(f"""
            CREATE OR REPLACE TABLE providers AS
            SELECT * FROM read_csv_auto('{filepath}', all_varchar=true)
        """)
    elif ext in (".xlsx", ".xls"):
        import pandas as pd
        xl = pd.ExcelFile(filepath)
        frames = []
        for sheet in xl.sheet_names:
            df = pd.read_excel(filepath, sheet_name=sheet, dtype=str).fillna("")
            df.columns = [c.upper().strip() for c in df.columns]
            if "NPI" in df.columns:
                frames.append(df)
        if not frames:
            raise ValueError("No sheets with NPI column found in Excel file.")
        combined = pd.concat(frames, ignore_index=True)
        con.register("_tmp", combined)
        con.execute("CREATE OR REPLACE TABLE providers AS SELECT * FROM _tmp")
    else:
        raise ValueError(f"Unsupported file type: {ext}")
    con.execute("CREATE INDEX IF NOT EXISTS idx_npi  ON providers(NPI)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_name ON providers(LAST_NAME)")
    count = con.execute("SELECT COUNT(*) FROM providers").fetchone()[0]
    con.close()
    return count

def build_context(p):
    denials = [k.upper() for k, v in [
        ("Part B",p["partb"]),("DME",p["dme"]),("HHA",p["hha"]),
        ("PMD",p["pmd"]),("Hospice",p["hospice"])] if not v]
    name = f"{p['first_name']} {p['last_name']}".strip()
    return (f"\n\n[PECOS DATABASE RECORD]\nProvider: {name}\nNPI: {p['npi']}\n"
            f"Part B: {'YES' if p['partb'] else 'NO'}\n"
            f"DME: {'YES' if p['dme'] else 'NO'}\n"
            f"HHA: {'YES' if p['hha'] else 'NO'}\n"
            f"PMD: {'YES' if p['pmd'] else 'NO'}\n"
            f"Hospice: {'YES' if p['hospice'] else 'NO'}\n"
            f"Denial risk: {', '.join(denials) if denials else 'None'}\n")

SYSTEM = """You are ClearPath, a Medicare provider eligibility expert for United Health Partners.
Help billing teams verify provider authorizations before submitting claims.
Five categories: Part B (labs/imaging), DME (equipment), HHA (home health), PMD (power mobility), Hospice.
When PECOS data is embedded: lead with YES/NO, explain denial implications clearly, suggest next steps.
Keep responses to 2-3 short paragraphs. Plain English. Don't reproduce the raw Y/N table."""

# ── Portfolio routes ──────────────────────────────────────────────────────────
@app.route("/")
def index():
    stats = get_stats()
    return render_template("index.html", stats=stats,
                           provider_count=f"{stats.get('total',0):,}" if stats else "—")

# ── ClearPath tool routes ─────────────────────────────────────────────────────
@app.route("/tools/clearpath")
def clearpath():
    stats = get_stats()
    return render_template("clearpath.html",
                           provider_count=f"{stats.get('total',0):,}" if stats else "—")

@app.route("/api/lookup")
def api_lookup():
    npi  = request.args.get("npi","").strip()
    name = request.args.get("name","").strip()
    if npi:
        p = lookup_npi(npi)
        return jsonify({"results":[p] if p else [],"found":bool(p)})
    if name:
        results = search_name(name)
        return jsonify({"results":results,"found":bool(results)})
    return jsonify({"error":"Provide npi or name"}), 400

@app.route("/api/chat", methods=["POST"])
def api_chat():
    body    = request.get_json() or {}
    message = body.get("message","").strip()
    history = body.get("history",[])
    if not message: return jsonify({"error":"No message"}), 400
    npi_match = re.search(r"\b1\d{9}\b", message)
    provider  = None
    if npi_match:
        provider = lookup_npi(npi_match.group(0))
        if provider:
            message += build_context(provider)
        else:
            message += f"\n\n[PECOS DATABASE RECORD]\nNPI {npi_match.group(0)}: NOT FOUND."
    resp = client.messages.create(
        model="claude-opus-4-5", max_tokens=600, system=SYSTEM,
        messages=history+[{"role":"user","content":message}]
    )
    return jsonify({"reply":resp.content[0].text,"provider":provider})

@app.route("/api/stats")
def api_stats():
    return jsonify(get_stats())

@app.route("/api/download-excel")
def download_excel():
    return send_file(XL_PATH, as_attachment=True, download_name="providers_summary.xlsx")

# ── Admin routes ──────────────────────────────────────────────────────────────
@app.route("/admin/login", methods=["GET","POST"])
def admin_login():
    if request.method == "POST":
        pw = request.form.get("password","")
        if pw == ADMIN_PASSWORD:
            session["admin"] = True
            return redirect(url_for("admin_dashboard"))
        flash("Incorrect password.")
    return render_template("admin_login.html")

@app.route("/admin/logout")
def admin_logout():
    session.pop("admin", None)
    return redirect(url_for("index"))

@app.route("/admin", methods=["GET"])
@login_required
def admin_dashboard():
    db_size = f"{os.path.getsize(DB_PATH)/1024/1024:.1f} MB" if db_exists() else "Not built"
    stats   = get_stats()
    return render_template("admin.html", db_size=db_size, stats=stats)

@app.route("/admin/upload", methods=["POST"])
@login_required
def admin_upload():
    if "datafile" not in request.files:
        return jsonify({"error":"No file uploaded"}), 400
    f = request.files["datafile"]
    if not f.filename:
        return jsonify({"error":"No file selected"}), 400
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in (".csv",".xlsx",".xls"):
        return jsonify({"error":"Only CSV or Excel files accepted"}), 400
    filename = secure_filename(f.filename)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    f.save(filepath)
    try:
        count = build_db_from_file(filepath)
        os.remove(filepath)
        return jsonify({"success":True, "count":count,
                        "message":f"Database rebuilt with {count:,} providers."})
    except Exception as e:
        return jsonify({"error":str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=5050)

# Jinja2 filter for number formatting
@app.template_filter('format_number')
def format_number(value):
    try:
        return f"{int(value):,}"
    except:
        return value
