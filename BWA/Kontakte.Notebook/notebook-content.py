# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "362c1572-1126-49df-ac93-d05f16dcc347",
# META       "default_lakehouse_name": "BWA_LH",
# META       "default_lakehouse_workspace_id": "273e76c4-5546-4215-8ae9-07e4e03a2182",
# META       "known_lakehouses": [
# META         {
# META           "id": "362c1572-1126-49df-ac93-d05f16dcc347"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
import pandas as pd
from io import StringIO

# ✅ Step 1: URL and credentials
url = "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obvgfrgcnlggywweodfhawtiyzvgewtqmzwgqwtinrqmnrtsmlbgazwkma/Kontakte.html"
username = "test"
password = "test"

# ✅ Step 2: Download the data
response = requests.get(url, auth=(username, password))

if response.status_code == 200:
    print("✅ Successfully downloaded the file")

    # Try reading as CSV, fallback to HTML
    try:
        df = pd.read_csv(StringIO(response.text), sep=';', encoding='latin1')
    except Exception:
        print("⚠️ Could not read as CSV — trying as HTML table instead...")
        df = pd.read_html(response.text, header=0, decimal=',', thousands='.')[0]

    # ✅ Step 3: Clean column names
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.replace(" ", "_")
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
        .str.lower()
    )

    # ✅ Step 4: Handle duplicate column names manually
    def make_unique(cols):
        seen = {}
        new_cols = []
        for c in cols:
            if c in seen:
                seen[c] += 1
                new_cols.append(f"{c}_{seen[c]}")
            else:
                seen[c] = 0
                new_cols.append(c)
        return new_cols

    df.columns = make_unique(df.columns)

    # ✅ Step 5: Convert all to string to prevent datatype conflicts
    df = df.astype(str)

    # ✅ Step 6: Drop table if exists to avoid schema conflict
    spark.sql("DROP TABLE IF EXISTS Kontakte")

    # ✅ Step 7: Save to Lakehouse
    spark.createDataFrame(df).write.mode("overwrite").saveAsTable("Kontakte")

    print("✅ Data successfully written to Lakehouse table 'Kontakte'")

else:
    print(f"❌ Download failed: {response.status_code}")
    print(response.text[:500])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd
from io import StringIO, BytesIO
from pyspark.sql import SparkSession

# ---------- CONFIG (only change these if needed) ----------
URL          = "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obvgfrgcnlggywweodfhawtiyzvgewtqmzwgqwtinrqmnrtsmlbgazwkma/Kontakte.html"
USERNAME     = "test"
PASSWORD     = "test"
TARGET_TABLE = "kontakte"     # Lakehouse table name
BUSINESS_KEY = "master_id"            # unique key, case-insensitive
CSV_SEPARATORS = [";", ","]          # will try in this order before HTML
# -----------------------------------------------------------

# Reuse Fabric session (safe), or create if missing
spark = SparkSession.getActiveSession() or SparkSession.builder.appName("Incremental HTML/CSV Loader").getOrCreate()

def clean_colname(name: str) -> str:
    """Make a safe Spark column name: strip, spaces->underscore, keep [A-Za-z0-9_] only."""
    name = (name or "").strip().replace(" ", "_")
    return "".join(ch if (ch.isalnum() or ch == "_") else "" for ch in name)

def download_to_pandas(url: str, user: str = None, pwd: str = None) -> pd.DataFrame:
    """Try CSV first (auto separators), then HTML table. Returns a pandas DataFrame of strings."""
    resp = requests.get(url, auth=(user, pwd) if user or pwd else None, timeout=60)
    resp.raise_for_status()
    text = resp.text

    # Try CSV with each separator
    for sep in CSV_SEPARATORS:
        try:
            df = pd.read_csv(StringIO(text), sep=sep, dtype=str)
            break
        except Exception:
            df = None

    # Fallback to HTML
    if df is None:
        tables = pd.read_html(text, header=0)
        if not tables:
            raise ValueError("No table found in the response.")
        df = tables[0].astype(str)

    # Standardize: all strings & strip
    df = df.astype(str)
    for c in df.columns:
        df[c] = df[c].str.strip()

    # Clean column names
    df.columns = [clean_colname(c) for c in df.columns]
    return df

# 1) Get pandas DataFrame dynamically (CSV/HTML)
pdf = download_to_pandas(URL, USERNAME, PASSWORD)

# 2) Resolve business key (case-insensitive) and filter blanks
key_col = next((c for c in pdf.columns if c.lower() == BUSINESS_KEY.lower()), None)
if key_col is None:
    raise ValueError(f"Business key '{BUSINESS_KEY}' not found. Columns: {list(pdf.columns)}")

pdf[key_col] = pdf[key_col].astype(str).str.strip()
pdf = pdf[pdf[key_col].notna() & (pdf[key_col] != "")]

# 3) To Spark DF
df_new = spark.createDataFrame(pdf)

# 4) Incremental logic: full load (create) or append only new keys
if spark.catalog.tableExists(TARGET_TABLE):
    # Read only existing keys (fast)
    df_existing_keys = spark.table(TARGET_TABLE).select(key_col).distinct()

    # Keep only rows with brand-new MasterID
    df_new_filtered = df_new.join(df_existing_keys, on=key_col, how="left_anti").dropDuplicates([key_col])

    # Append if new rows exist (Writer V2)
    if df_new_filtered.head(1):
        df_new_filtered.writeTo(TARGET_TABLE).append()
        print(f"✅ Appended {df_new_filtered.count()} new rows to '{TARGET_TABLE}'.")
    else:
        print("✅ No new rows to append — all MasterID values already exist.")
else:
    # First run / table deleted: create as managed Delta table (Writer V2)
    base = df_new.dropDuplicates([key_col])
    base.writeTo(TARGET_TABLE).using("delta").create()
    print(f"✅ Full load completed — table '{TARGET_TABLE}' created with {base.count()} rows.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
