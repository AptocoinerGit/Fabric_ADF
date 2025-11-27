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
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from io import BytesIO

# Use existing Spark session
try:
    spark
except NameError:
    spark = SparkSession.builder \
        .appName("Incremental Load") \
        .getOrCreate()

# Dropbox file URL
url = "https://www.dropbox.com/scl/fi/siyk65ywhbw03knxo0hya/Book1.xlsx?rlkey=ssb17z3qtrdaik8jspjp73k0y&st=d3xja6s4&dl=1"

# Business key
business_key = "MasterID"

# Table name
table_name = "incremental_table"

try:
    # Download file from Dropbox
    response = requests.get(url)
    response.raise_for_status()
    
    # Read Excel file using pandas
    excel_data = pd.read_excel(BytesIO(response.content))
    
    # Convert all columns to string to avoid type conversion issues
    excel_data = excel_data.astype(str)
    
    # Convert to Spark DataFrame
    df_new = spark.createDataFrame(excel_data)
    
    # Check if table exists
    try:
        spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
        table_exists = True
    except:
        table_exists = False
    
    if not table_exists:
        # Full load if table doesn't exist
        df_new.write.mode("overwrite").format("delta").saveAsTable(table_name)
        print("Full load completed - Table created")
    else:
        # Get existing data
        df_existing = spark.table(table_name)
        
        # Find new records based on business key
        df_new_filtered = df_new.join(
            df_existing.select(business_key), 
            on=business_key, 
            how="left_anti"
        )
        
        # Combine existing and new data
        df_combined = df_existing.union(df_new_filtered)
        
        # Overwrite the table with combined data
        df_combined.write.mode("overwrite").format("delta").saveAsTable(table_name)
        print(f"Updated table with {df_new_filtered.count()} new records")
            
except Exception as e:
    print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd
from io import BytesIO
from pyspark.sql import SparkSession

# ---------------- CONFIG ----------------
DROPBOX_URL  = "https://www.dropbox.com/scl/fi/siyk65ywhbw03knxo0hya/Book1.xlsx?rlkey=ssb17z3qtrdaik8jspjp73k0y&st=d3xja6s4&dl=1"
TABLE_NAME   = "incremental_table"   # Lakehouse managed table
BUSINESS_KEY = "MasterID"            # unique key column
# ----------------------------------------

# 1️⃣ Reuse or create Spark session
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# 2️⃣ Read Excel from Dropbox (via pandas)
resp = requests.get(DROPBOX_URL, timeout=120)
resp.raise_for_status()
pdf = pd.read_excel(BytesIO(resp.content), dtype=str)
pdf = pdf.fillna("")        # avoid null type mix
pdf.columns = [c.strip().replace(" ", "_") for c in pdf.columns]

# 3️⃣ Ensure business key column exists
key_col = next((c for c in pdf.columns if c.lower() == BUSINESS_KEY.lower()), None)
if key_col is None:
    raise ValueError(f"Business key '{BUSINESS_KEY}' not found in columns {list(pdf.columns)}")

pdf[key_col] = pdf[key_col].astype(str).str.strip()
pdf = pdf[pdf[key_col] != ""]

# 4️⃣ To Spark DF
df_new = spark.createDataFrame(pdf)

# 5️⃣ Incremental logic
if spark.catalog.tableExists(TABLE_NAME):
    # Read only existing keys
    df_existing_keys = spark.table(TABLE_NAME).select(key_col).distinct()

    # Keep rows that are new (anti join)
    df_new_filtered = df_new.join(df_existing_keys, on=key_col, how="left_anti")

    new_count = df_new_filtered.count()
    if new_count > 0:
        # ✅ Append new rows (Writer V2 API)
        df_new_filtered.writeTo(TABLE_NAME).append()
        print(f"✅ Added {new_count} new rows to '{TABLE_NAME}'.")
    else:
        print("✅ No new rows to add – all MasterID values already exist.")
else:
    # ✅ First run or table deleted → full load (Writer V2 API)
    df_new.dropDuplicates([key_col]) \
          .writeTo(TABLE_NAME) \
          .using("delta") \
          .create()
    print(f"✅ Full load completed – table '{TABLE_NAME}' created with {df_new.count()} rows.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CONFIG ---
DROPBOX_URL   = "https://www.dropbox.com/scl/fi/siyk65ywhbw03knxo0hya/Book1.xlsx?rlkey=ssb17z3qtrdaik8jspjp73k0y&st=d3xja6s4&dl=0"
TARGET_TABLE  = "book1_delta"        # managed table in default Lakehouse
BUSINESS_KEY  = "MasterID"           # unique key (case-insensitive)
SHEET_NAME    = None                 # or "Sheet1"
LAKEHOUSE_DB  = None                 # e.g. "bwa_lh"; keep None to use current default
# ---------------

import io, re, requests, pandas as pd
from pyspark.sql import SparkSession

def get_spark() -> SparkSession:
    """
    In managed environments, simply relying on getOrCreate() is the most robust way
    to ensure you use the active session or create a new one safely.
    """
    return SparkSession.builder.appName("Fabric-Incremental-Append").getOrCreate()

# Ensure you are still running your code *without* spark.stop() at the end.
    try:
        s = SparkSession.getActiveSession()
        if s is not None:
            # try to detect stopped context
            try:
                if not s.sparkContext._jsc.sc().isStopped():
                    return s
            except Exception:
                # if we can't query isStopped, still try using it
                return s
    except Exception:
        pass
    # Create or rehydrate a new session
    return SparkSession.builder.appName("Fabric-Incremental-Append").getOrCreate()

spark = get_spark()
if LAKEHOUSE_DB:
    # Set Lakehouse database via Spark API (no SQL)
    spark.catalog.setCurrentDatabase(LAKEHOUSE_DB)

def direct_dl(url: str) -> str:
    if "dl=0" in url: return url.replace("dl=0", "dl=1")
    if "dl=1" in url: return url
    return url + ("&" if "?" in url else "?") + "dl=1"

def clean_name(c: str) -> str:
    c = (c or "").strip()
    c = re.sub(r"\s+", "_", c)
    c = re.sub(r"[^\w]", "_", c)
    return c

# 1) Download Excel
resp = requests.get(direct_dl(DROPBOX_URL), stream=True, timeout=120)
resp.raise_for_status()
bio = io.BytesIO(resp.content)

# 2) Read Excel with pandas (robust in Fabric)
pdf = pd.read_excel(bio, sheet_name=SHEET_NAME or 0, dtype=str)

# 3) Trim strings (no deprecated applymap; column-wise)
for col in pdf.columns:
    if pdf[col].dtype == "object":
        pdf[col] = pdf[col].str.strip()

# 4) Clean column names
pdf.columns = [clean_name(c) for c in pdf.columns]

# 5) Ensure business key exists and is valid
key_col = next((c for c in pdf.columns if c.lower() == BUSINESS_KEY.lower()), None)
if key_col is None:
    raise ValueError(f"Business key '{BUSINESS_KEY}' not found. Columns: {list(pdf.columns)}")

pdf[key_col] = pdf[key_col].astype(str).str.strip()
pdf = pdf[pdf[key_col].notna() & (pdf[key_col] != "")]

# 6) To Spark DF
df = spark.createDataFrame(pdf)

# 7) Incremental append-only logic (no duplicates)
if spark.catalog.tableExists(TARGET_TABLE):
    # ... (logic for appending new rows)
    if new_rows.head(1):
        new_rows.writeTo(TARGET_TABLE).append()
        print(f"✅ Appended {new_rows.count()} new rows to '{TARGET_TABLE}'.")
    else:
        print("✅ No new rows to append — all MasterID values already exist.")
else:
    # First run (or table re-created): full load as managed Delta table
    base = df.dropDuplicates([key_col])
    base.writeTo(TARGET_TABLE).using("delta").create()
    print(f"✅ Created table '{TARGET_TABLE}' with {base.count()} rows (full load).")

# spark.stop()  <--- REMOVE THIS LINE
# print("spark session is stoped") <--- REMOVE THIS LINE

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd
from io import StringIO

# --- 1️⃣ Download data ---
url = "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obumvsggnrqmuwtimrwmuwtizdegqwweodgmywtaztcgzswenjrmqydomy/Buchungen_Basis.html"
username = "test"
password = "test"

response = requests.get(url, auth=(username, password))

if response.status_code != 200:
    print(f"❌ Download failed: {response.status_code}")
    print(response.text[:500])
    raise SystemExit()

print("✅ Successfully downloaded the file")

# --- 2️⃣ Read data into DataFrame ---
try:
    df = pd.read_csv(StringIO(response.text), sep=';', encoding='latin1')
except Exception:
    print("Trying as HTML table instead...")
    df = pd.read_html(response.text, header=0)[0]

# --- 3️⃣ Clean column names ---
df.columns = (
    df.columns.str.strip()
    .str.replace(" ", "_")
    .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
)

# --- 4️⃣ Clean numeric columns ---
numeric_cols = ["Soll", "Haben", "Betrag"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

print(f"📄 Total records downloaded: {len(df)}")

# --- 5️⃣ Incremental Load Logic ---

table_name = "Buchungen_Basis11"

# Check if Lakehouse table exists
try:
    existing_df = spark.table(table_name).toPandas()
    print(f"📦 Existing records in Lakehouse: {len(existing_df)}")

    # Compare MasterID column
    if "MasterID" not in df.columns:
        raise KeyError("❌ 'MasterID' column not found in downloaded data")

    if "MasterID" not in existing_df.columns:
        raise KeyError("❌ 'MasterID' column not found in existing Lakehouse table")

    # Find new records
    new_records = df[~df["MasterID"].isin(existing_df["MasterID"])]

    if not new_records.empty:
        print(f"🆕 New records to insert: {len(new_records)}")
        spark.createDataFrame(new_records).write.mode("append").saveAsTable(table_name)
    else:
        print("✅ No new records to insert. Lakehouse is up to date.")

except Exception as e:
    # If table doesn't exist, full load
    print(f"⚙️ Full load (first run or error): {e}")
    spark.createDataFrame(df).write.mode("overwrite").saveAsTable(table_name)
    print(f"✅ Full load completed with {len(df)} records.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
