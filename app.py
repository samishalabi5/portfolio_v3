"""
S. Shalabi — Data Analytics & AI Portfolio
Flask app: portfolio site + MediLens provider intelligence tool + admin
"""

import os, re, secrets
from functools import wraps
from flask import (Flask, render_template, request, jsonify,
                   send_file, redirect, url_for, session, flash)
import duckdb
from anthropic import Anthropic
from werkzeug.utils import secure_filename

app    = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))
client = Anthropic()

BASE          = os.path.dirname(__file__)
DB_PATH       = os.path.join(BASE, "medilens.db")
XL_PATH       = os.path.join(BASE, "providers_database.xlsx")
UPLOAD_FOLDER = os.path.join(BASE, "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "clearpath2026")

# ── Auth ──────────────────────────────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return decorated

# ── DB helpers ────────────────────────────────────────────────────────────────
def get_db():
    return duckdb.connect(DB_PATH, read_only=True)

def db_exists():
    return os.path.exists(DB_PATH)

def safe_float(v):
    try: return float(v) if v and str(v) not in ('','nan','None') else None
    except: return None

def safe_int(v):
    try: return int(float(v)) if v and str(v) not in ('','nan','None') else None
    except: return None

def lookup_provider(npi):
    if not db_exists(): return None
    with get_db() as con:
        row = con.execute("""
            SELECT p.NPI, p.LAST_NAME, p.FIRST_NAME,
                p.PARTB, p.DME as DME_ELIG, p.HHA, p.PMD, p.HOSPICE,
                ph.Rndrng_Prvdr_Type, ph.Rndrng_Prvdr_State_Abrvtn, ph.Rndrng_Prvdr_City,
                ph.Rndrng_Prvdr_Zip5, ph.Rndrng_Prvdr_Crdntls,
                ph.Tot_Benes, ph.Tot_Srvcs, ph.Tot_Mdcr_Pymt_Amt, ph.Tot_Sbmtd_Chrg,
                ph.Bene_Avg_Age, ph.Bene_Feml_Cnt, ph.Bene_Male_Cnt, ph.Bene_Dual_Cnt,
                ph.Bene_CC_PH_Cancer6_V2_Pct, ph.Bene_CC_BH_Alz_NonAlzdem_V2_Pct,
                ph.Bene_CC_PH_COPD_V2_Pct, ph.Bene_CC_PH_Diabetes_V2_Pct,
                ph.Bene_CC_PH_HF_NonIHD_V2_Pct, ph.Bene_CC_PH_CKD_V2_Pct,
                ph.Bene_Avg_Risk_Scre, ph.Rndrng_Prvdr_Mdcr_Prtcptg_Ind,
                d.Tot_Suplr_Clms, d.Suplr_Mdcr_Pymt_Amt,
                d.DME_Tot_Suplr_Clms, d.DME_Suplr_Mdcr_Pymt_Amt, d.DME_Tot_Suplr_Benes,
                d.POS_Tot_Suplr_Clms, d.POS_Suplr_Mdcr_Pymt_Amt
            FROM pecos p
            LEFT JOIN phys ph ON p.NPI = ph.Rndrng_NPI
            LEFT JOIN dme d ON p.NPI = d.Rfrg_NPI
            WHERE p.NPI = ?
        """, [npi.strip()]).fetchone()
        if not row: return None
        cols = ['npi','last_name','first_name','partb','dme_elig','hha','pmd','hospice',
                'specialty','state','city','zip','credentials','total_patients','total_services',
                'total_medicare_payments','total_submitted_charges','avg_patient_age',
                'female_patients','male_patients','dual_eligible','pct_cancer','pct_dementia',
                'pct_copd','pct_diabetes','pct_heart_failure','pct_ckd','avg_risk_score',
                'participating','dme_total_claims','dme_total_payments','dme_equip_claims',
                'dme_equip_payments','dme_patients','pos_claims','pos_payments']
        p = dict(zip(cols, row))
        if p.get('specialty'):
            bench = con.execute("""
                SELECT peer_count, avg_patients, avg_medicare_payments, median_medicare_payments,
                       avg_dme_payments, p75_dme_payments, p90_dme_payments,
                       avg_patient_age, avg_pct_cancer, avg_pct_dementia
                FROM specialty_benchmarks WHERE specialty = ?
            """, [p['specialty']]).fetchone()
            if bench:
                p['benchmarks'] = dict(zip(['peer_count','avg_patients','avg_medicare_payments',
                    'median_medicare_payments','avg_dme_payments','p75_dme_payments',
                    'p90_dme_payments','avg_patient_age','avg_pct_cancer','avg_pct_dementia'],
                    [safe_float(x) for x in bench]))
        return p

def search_providers(last='', state='', sort_by='name', limit=20):
    conditions, params = [], []
    if last:
        conditions.append("p.LAST_NAME LIKE ?")
        params.append(last.upper().strip() + '%')
    if state:
        conditions.append("ph.Rndrng_Prvdr_State_Abrvtn = ?")
        params.append(state.upper())
    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
    sort_map = {
        'name':     'p.LAST_NAME ASC',
        'payments': 'TRY_CAST(ph.Tot_Mdcr_Pymt_Amt AS DOUBLE) DESC NULLS LAST',
        'dme':      'TRY_CAST(d.DME_Suplr_Mdcr_Pymt_Amt AS DOUBLE) DESC NULLS LAST',
        'patients': 'TRY_CAST(ph.Tot_Benes AS DOUBLE) DESC NULLS LAST',
        'risk':     'TRY_CAST(ph.Bene_Avg_Risk_Scre AS DOUBLE) DESC NULLS LAST',
    }
    order = sort_map.get(sort_by, 'p.LAST_NAME ASC')
    if not db_exists(): return []
    with get_db() as con:
        rows = con.execute(f"""
            SELECT p.NPI, p.LAST_NAME, p.FIRST_NAME, p.PARTB, p.DME, p.HHA, p.PMD, p.HOSPICE,
                   ph.Rndrng_Prvdr_Type, ph.Rndrng_Prvdr_State_Abrvtn, ph.Rndrng_Prvdr_City,
                   TRY_CAST(ph.Tot_Mdcr_Pymt_Amt AS DOUBLE),
                   TRY_CAST(d.DME_Suplr_Mdcr_Pymt_Amt AS DOUBLE),
                   TRY_CAST(ph.Tot_Benes AS DOUBLE)
            FROM pecos p
            LEFT JOIN phys ph ON p.NPI = ph.Rndrng_NPI
            LEFT JOIN dme d ON p.NPI = d.Rfrg_NPI
            {where} ORDER BY {order} LIMIT ?
        """, params + [limit]).fetchall()
    return [{'npi':r[0],'last':r[1],'first':r[2],'partb':r[3],'dme':r[4],'hha':r[5],
             'pmd':r[6],'hospice':r[7],'specialty':r[8] or '—','state':r[9] or '',
             'city':r[10] or '','payments':safe_float(r[11]),'dme_payments':safe_float(r[12]),
             'patients':safe_int(r[13])} for r in rows]

def get_states():
    if not db_exists(): return []
    with get_db() as con:
        rows = con.execute("""SELECT DISTINCT Rndrng_Prvdr_State_Abrvtn FROM phys
            WHERE Rndrng_Prvdr_State_Abrvtn IS NOT NULL AND Rndrng_Prvdr_State_Abrvtn != ''
            ORDER BY 1""").fetchall()
    return [r[0] for r in rows]

def get_overview_stats():
    if not db_exists(): return {}
    with get_db() as con:
        r = con.execute("""SELECT
            (SELECT COUNT(*) FROM pecos),
            (SELECT COUNT(*) FROM dme),
            (SELECT COUNT(*) FROM phys),
            (SELECT SUM(TRY_CAST(Suplr_Mdcr_Pymt_Amt AS DOUBLE)) FROM dme),
            (SELECT SUM(TRY_CAST(Tot_Mdcr_Pymt_Amt AS DOUBLE)) FROM phys),
            (SELECT COUNT(*) FROM pecos WHERE HOSPICE='Y'),
            (SELECT COUNT(*) FROM pecos WHERE DME='Y')""").fetchone()
    return {'pecos_total':safe_int(r[0]),'dme_total':safe_int(r[1]),'phys_total':safe_int(r[2]),
            'total_dme_payments':safe_float(r[3]),'total_phys_payments':safe_float(r[4]),
            'hospice_eligible':safe_int(r[5]),'dme_eligible':safe_int(r[6])}

def get_top_dme_specialties():
    if not db_exists(): return []
    with get_db() as con:
        rows = con.execute("""SELECT Rfrg_Prvdr_Spclty_Desc, COUNT(*) as c,
            SUM(TRY_CAST(DME_Suplr_Mdcr_Pymt_Amt AS DOUBLE)) as t,
            AVG(TRY_CAST(DME_Suplr_Mdcr_Pymt_Amt AS DOUBLE)) as a
            FROM dme WHERE Rfrg_Prvdr_Spclty_Desc IS NOT NULL AND Rfrg_Prvdr_Spclty_Desc != ''
            GROUP BY Rfrg_Prvdr_Spclty_Desc ORDER BY t DESC LIMIT 12""").fetchall()
    return [{'specialty':r[0],'count':safe_int(r[1]),'total':safe_float(r[2]),'avg':safe_float(r[3])} for r in rows]

def get_top_providers_by(metric='payments', state=None, limit=10):
    if not db_exists(): return []
    state_filter = "AND ph.Rndrng_Prvdr_State_Abrvtn = ?" if state else ""
    params = ([state] if state else []) + [limit]
    col_map = {
        'payments': 'TRY_CAST(ph.Tot_Mdcr_Pymt_Amt AS DOUBLE)',
        'dme':      'TRY_CAST(d.DME_Suplr_Mdcr_Pymt_Amt AS DOUBLE)',
        'patients': 'TRY_CAST(ph.Tot_Benes AS DOUBLE)',
        'risk':     'TRY_CAST(ph.Bene_Avg_Risk_Scre AS DOUBLE)',
    }
    col = col_map.get(metric, col_map['payments'])
    with get_db() as con:
        rows = con.execute(f"""
            SELECT p.NPI, p.LAST_NAME, p.FIRST_NAME, ph.Rndrng_Prvdr_Type,
                   ph.Rndrng_Prvdr_State_Abrvtn, ph.Rndrng_Prvdr_City, {col} as val
            FROM pecos p
            LEFT JOIN phys ph ON p.NPI = ph.Rndrng_NPI
            LEFT JOIN dme d ON p.NPI = d.Rfrg_NPI
            WHERE {col} IS NOT NULL {state_filter}
            ORDER BY val DESC NULLS LAST LIMIT ?
        """, params).fetchall()
    return [{'npi':r[0],'last':r[1],'first':r[2],'specialty':r[3] or '—',
             'state':r[4] or '','city':r[5] or '','value':safe_float(r[6])} for r in rows]

def build_context(p):
    denials = [l for k,l in [('partb','Part B'),('dme_elig','DME'),('hha','HHA'),('pmd','PMD'),('hospice','Hospice')] if p.get(k)!='Y']
    b = p.get('benchmarks', {})
    dme_p = safe_float(p.get('dme_equip_payments')) or 0
    avg_dme = b.get('avg_dme_payments') or 0
    dme_diff = ((dme_p/avg_dme-1)*100) if avg_dme > 0 else None
    tot_p = safe_float(p.get('total_medicare_payments')) or 0
    avg_tot = b.get('avg_medicare_payments') or 0
    tot_diff = ((tot_p/avg_tot-1)*100) if avg_tot > 0 else None
    name = f"{p.get('first_name','')} {p.get('last_name','')}".strip()
    avg_pts = b.get('avg_patients')
    return f"""
[MEDILENS PROVIDER INTELLIGENCE REPORT]
Provider: {name} {p.get('credentials','')}
NPI: {p['npi']} | Specialty: {p.get('specialty','Unknown')} | Location: {p.get('city','')}, {p.get('state','')}
PECOS: Part B:{'YES' if p.get('partb')=='Y' else 'NO'} DME:{'YES' if p.get('dme_elig')=='Y' else 'NO'} HHA:{'YES' if p.get('hha')=='Y' else 'NO'} PMD:{'YES' if p.get('pmd')=='Y' else 'NO'} Hospice:{'YES' if p.get('hospice')=='Y' else 'NO'}
Denial risk: {', '.join(denials) if denials else 'None'}
2023 BILLING: Patients:{p.get('total_patients','N/A')} Medicare payments:${tot_p:,.0f} ({f'+{tot_diff:.0f}%' if tot_diff and tot_diff>0 else f'{tot_diff:.0f}%' if tot_diff else 'N/A'} vs peers) Risk score:{p.get('avg_risk_score','N/A')}
DME: Payments:${dme_p:,.0f} ({f'+{dme_diff:.0f}%' if dme_diff and dme_diff>0 else f'{dme_diff:.0f}%' if dme_diff else 'N/A'} vs specialty avg)
CHRONIC CONDITIONS: Cancer:{p.get('pct_cancer','N/A')}% Dementia:{p.get('pct_dementia','N/A')}% COPD:{p.get('pct_copd','N/A')}%
PEERS ({p.get('specialty','')}, n={b.get('peer_count','N/A')}): Avg patients:{f"{avg_pts:,.0f}" if avg_pts else 'N/A'} Avg Medicare:${avg_tot:,.0f} Avg DME:${avg_dme:,.0f}
"""

SYSTEM = """You are MediLens, a Medicare analytics expert. Analyze provider billing, DME ordering, denial risk, and clinical profiles.
Be direct, 3-4 paragraphs max. Flag outliers vs peers. Don't reproduce raw numbers already in the UI."""

# ── Portfolio routes ──────────────────────────────────────────────────────────
@app.route("/")
def index():
    stats = get_overview_stats()
    total = stats.get('pecos_total', 0)
    return render_template("index.html",
                           provider_count=f"{total:,}" if total else "—")

# ── MediLens routes ───────────────────────────────────────────────────────────
@app.route("/tools/medilens")
def medilens():
    states = get_states()
    return render_template("medilens.html", states=states)

@app.route("/api/lookup")
def api_lookup():
    npi     = request.args.get("npi","").strip()
    name    = request.args.get("name","").strip()
    state   = request.args.get("state","").strip()
    sort_by = request.args.get("sort","name")
    if npi:
        p = lookup_provider(npi)
        return jsonify({"result":p,"found":bool(p)})
    results = search_providers(last=name, state=state, sort_by=sort_by)
    return jsonify({"results":results,"found":bool(results)})

@app.route("/api/chat", methods=["POST"])
def api_chat():
    body    = request.get_json() or {}
    message = body.get("message","").strip()
    history = body.get("history",[])
    if not message: return jsonify({"error":"No message"}), 400
    npi_match = re.search(r"\b1\d{9}\b", message)
    provider  = None
    if npi_match:
        provider = lookup_provider(npi_match.group(0))
        if provider: message += build_context(provider)
        else: message += f"\n\n[NPI {npi_match.group(0)}: NOT FOUND in PECOS]"
    resp = client.messages.create(
        model="claude-opus-4-5", max_tokens=700, system=SYSTEM,
        messages=history + [{"role":"user","content":message}]
    )
    return jsonify({"reply":resp.content[0].text,"provider":provider})

@app.route("/api/stats")
def api_stats():
    return jsonify(get_overview_stats())

@app.route("/api/top-specialties")
def api_top_specialties():
    return jsonify(get_top_dme_specialties())

@app.route("/api/top-providers")
def api_top_providers():
    metric = request.args.get("metric","payments")
    state  = request.args.get("state","") or None
    return jsonify(get_top_providers_by(metric=metric, state=state))

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
    db_size = f"{os.path.getsize(DB_PATH)/1024/1024:.0f} MB" if db_exists() else "Not built"
    stats   = get_overview_stats()
    return render_template("admin.html", db_size=db_size, stats=stats)

@app.route("/admin/upload", methods=["POST"])
@login_required
def admin_upload():
    if "datafile" not in request.files:
        return jsonify({"error":"No file uploaded"}), 400
    f = request.files["datafile"]
    if not f.filename: return jsonify({"error":"No file selected"}), 400
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in (".csv",".xlsx",".xls"):
        return jsonify({"error":"Only CSV or Excel accepted"}), 400
    filename = secure_filename(f.filename)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    f.save(filepath)
    try:
        # Rebuild MediLens DB from uploaded file
        import pandas as pd
        con = duckdb.connect(DB_PATH)
        if ext == ".csv":
            con.execute(f"CREATE OR REPLACE TABLE pecos AS SELECT * FROM read_csv_auto('{filepath}', all_varchar=true)")
        else:
            xl = pd.ExcelFile(filepath)
            frames = [pd.read_excel(filepath, sheet_name=s, dtype=str).fillna("") for s in xl.sheet_names]
            combined = pd.concat([df for df in frames if "NPI" in [c.upper() for c in df.columns]], ignore_index=True)
            combined.columns = [c.upper().strip() for c in combined.columns]
            con.register("_tmp", combined)
            con.execute("CREATE OR REPLACE TABLE pecos AS SELECT * FROM _tmp")
        con.execute("CREATE INDEX IF NOT EXISTS idx_pecos_npi ON pecos(NPI)")
        count = con.execute("SELECT COUNT(*) FROM pecos").fetchone()[0]
        con.close()
        os.remove(filepath)
        return jsonify({"success":True,"count":count,"message":f"PECOS table rebuilt with {count:,} providers."})
    except Exception as e:
        return jsonify({"error":str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5050)), debug=False)

@app.template_filter('format_number')
def format_number(value):
    try: return f"{int(value):,}"
    except: return value
