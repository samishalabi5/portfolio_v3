"""
setup_db.py  —  Run this ONCE to build providers.db from your source data.

Usage:
    python setup_db.py --csv  path/to/OrderReferring.csv
    python setup_db.py --excel path/to/providers.xlsx   (uses first two sheets)

The script:
  1. Reads the source file (CSV or Excel)
  2. Loads all rows into DuckDB
  3. Creates NPI + LAST_NAME indexes for fast lookups
  4. Prints query benchmark results

After setup_db.py completes, run:  python app.py
"""

import argparse, os, time, duckdb

DB_PATH = os.path.join(os.path.dirname(__file__), "providers.db")

def build_from_csv(csv_path: str):
    print(f"Reading CSV: {csv_path}")
    con = duckdb.connect(DB_PATH)
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE providers AS
        SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true)
    """)
    print(f"  Loaded in {time.time()-t0:.1f}s")
    _index_and_verify(con)

def build_from_excel(xlsx_path: str):
    """Reads Excel via pandas then bulk-inserts into DuckDB."""
    import pandas as pd
    print(f"Reading Excel: {xlsx_path}")
    xl = pd.ExcelFile(xlsx_path)
    print(f"  Sheets found: {xl.sheet_names}")

    frames = []
    for sheet in xl.sheet_names:
        df = pd.read_excel(xlsx_path, sheet_name=sheet, dtype=str).fillna("")
        # Normalise column names to upper
        df.columns = [c.upper().strip() for c in df.columns]
        if "NPI" in df.columns:
            frames.append(df)
            print(f"  {sheet}: {len(df):,} rows")

    if not frames:
        raise ValueError("No sheets with an NPI column found.")

    import pandas as pd
    combined = pd.concat(frames, ignore_index=True)
    print(f"  Total rows: {len(combined):,}")

    con = duckdb.connect(DB_PATH)
    t0 = time.time()
    con.register("_tmp", combined)
    con.execute("CREATE OR REPLACE TABLE providers AS SELECT * FROM _tmp")
    print(f"  Loaded into DuckDB in {time.time()-t0:.1f}s")
    _index_and_verify(con)

def _index_and_verify(con):
    print("Building indexes…")
    con.execute("CREATE INDEX IF NOT EXISTS idx_npi  ON providers(NPI)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_name ON providers(LAST_NAME)")

    count = con.execute("SELECT COUNT(*) FROM providers").fetchone()[0]
    print(f"  Total rows indexed: {count:,}")

    # Benchmark
    t = time.time()
    r = con.execute("SELECT * FROM providers WHERE NPI = '1497824817'").fetchone()
    print(f"  NPI lookup: {(time.time()-t)*1000:.1f}ms  → {r}")

    t = time.time()
    n = con.execute("SELECT COUNT(*) FROM providers WHERE LAST_NAME LIKE 'SMITH%'").fetchone()[0]
    print(f"  Name search 'SMITH%': {(time.time()-t)*1000:.0f}ms  → {n} results")

    sz = os.path.getsize(DB_PATH)
    print(f"\n✓ providers.db created ({sz/1024/1024:.1f} MB)")
    print("  Run:  python app.py")
    con.close()

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Build DuckDB from CMS PECOS source data")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--csv",   metavar="FILE", help="Path to CSV source file")
    g.add_argument("--excel", metavar="FILE", help="Path to Excel source file (.xlsx)")
    args = p.parse_args()

    if args.csv:
        build_from_csv(args.csv)
    else:
        build_from_excel(args.excel)
