# S. Shalabi — Data Analytics & AI Portfolio

A full-stack portfolio site with live AI-powered analytics tools, built for the
United Health Partners AI Data Analytics & Informatics internship application.

## Structure

```
app.py                    ← Single Flask app (portfolio + tools + admin)
providers.db              ← DuckDB database (build with setup_db.py)
providers_database.xlsx   ← Excel summary workbook
setup_db.py               ← One-time database builder
requirements.txt
templates/
  index.html              ← Portfolio homepage
  clearpath.html          ← ClearPath tool page
  admin_login.html        ← Admin login
  admin.html              ← Admin dashboard (Excel upload)
uploads/                  ← Temp folder for uploaded files
```

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Set environment variables
```bash
# Required
set ANTHROPIC_API_KEY=sk-ant-...

# Optional (defaults shown)
set ADMIN_PASSWORD=clearpath2026
set SECRET_KEY=any-random-string
```

### 3. Build the database
```bash
python setup_db.py --csv OrderReferring_20260413.csv
```

### 4. Run
```bash
python app.py
```

Open **http://localhost:5050**

## Pages

| URL | Description |
|---|---|
| `/` | Portfolio homepage |
| `/tools/clearpath` | ClearPath eligibility tool |
| `/admin` | Data management dashboard |
| `/admin/login` | Admin login (password: `clearpath2026`) |

## Updating the Data

1. Go to **http://localhost:5050/admin**
2. Log in with your admin password
3. Drag in a new Excel or CSV file from CMS
4. Click **Rebuild Database** — done in ~12 seconds

## Deploying to Railway

1. Push this folder to a GitHub repo
2. Go to **railway.app** → New Project → Deploy from GitHub
3. Add environment variables in Railway dashboard:
   - `ANTHROPIC_API_KEY`
   - `ADMIN_PASSWORD`
   - `SECRET_KEY`
4. Railway auto-deploys on every git push

Note: The `providers.db` file (163 MB) needs to be uploaded via the admin panel
after first deployment, or committed to the repo if under Railway's size limit.
