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

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

print(spark)

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
print(resp.content)

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
print(resp)
resp.raise_for_status()
print("response" ,resp.raise_for_status())
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

import requests
import pandas as pd
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==================== CONFIG ====================
# Put ALL your sources here. For each item:
#   name:        Lakehouse table name to write
#   url:         source URL (CSV/HTML)
#   auth:        ("user","pass") or None
#   key_candidates (optional): list of possible key column names you expect
SOURCES = [
    {
        "name": "Buchungen_Basis",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obumvsggnrqmuwtimrwmuwtizdegqwweodgmywtaztcgzswenjrmqydomy/Buchungen_Basis.html",
        "auth": ("test", "test"),
        "key_candidates": "MasterID",
    },
    {
        "name": "Buchungen_Basis_Budget",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6mrtgvsdsn3bmiwtmndgguwtizjrg4wwcmlegywwczbrgzrwknjvhbtdcyy/Buchungen_Basis_Budget.html",
        "auth":  ("test", "test")
    },
    
    {
        "name": "BWA_Gliederung",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6m3cgiygczlbg4wtgztdhewtimzwmiwtsytfhewtmojvguytonjymyytkoa/BWA_Gliederung.html",
        "auth": ("test", "test")
        # no key_candidates -> will auto-detect; if none found, will hash row
    },
    
    {
        "name": "Kontakte",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obvgfrgcnlggywweodfhawtiyzvgewtqmzwgqwtinrqmnrtsmlbgazwkma/Kontakte.html",
        "auth": ("test", "test"),
        "key_candidates": "master_id"
    },
    
    {
        "name": "Kreditoren",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6nztgqydomrsg4wtsntgmywtinzwgawtsnbygqwtcmrqgfsdkmzwgntgkzq/Kreditoren.html",
        "auth": ("test", "test"),
        "key_candidates": "Kontakt_MasterID",
    },
    
    {
        "name": "Kreditoren_Gesamtumsatz",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6mbugbsdknjymywtiyjvgewtimbwguwtqojxmiwtcmbtmu3wmzlemqzdmzq/Kreditoren_Gesamtumsatz_.html",
        "auth":("test", "test"),
         "key_candidates": "masterid",
        
        
    },
    
    {
        "name": "OP_Liste",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6y3emeytombxgqwwiyleguwtizbwmywwenrxmuwtczjrmy3tenbxguygimi/OP_Liste.html",
        "auth": ("test", "test"),
         "key_candidates": "masterid",
    },
    
    {
        "name": "SCRBWA_Gliederung",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6zbygfrdgmrqmiwtcmrtmuwtizbthewwcmrqgqwwcndbmiydmytfgy4doma/SCRBWA_Gliederung.html",
        "auth":  ("test", "test"),
         #no key_candidates -> will auto-detect; if none found, will hash row
    }
]

CSV_SEPARATORS = [";", ","]  # try in order, then fallback to HTML
# =================================================

# Reuse Fabric Spark (safe) or create if missing
spark = SparkSession.getActiveSession() or SparkSession.builder.appName("Multi-Incremental-Loader").getOrCreate()

# ---------- helpers ----------
def clean_colname(name: str) -> str:
    """Safe Spark column name: strip, spaces->underscore, keep [A-Za-z0-9_] only."""
    name = (name or "").strip().replace(" ", "_")
    return "".join(ch if (ch.isalnum() or ch == "_") else "" for ch in name)

def read_table_like(url: str, auth=None) -> pd.DataFrame:
    """Try CSV with common separators; fallback to first HTML table. Return pandas DF of strings with cleaned column names."""
    resp = requests.get(url, auth=auth, timeout=90)
    resp.raise_for_status()
    text = resp.text

    df = None
    for sep in CSV_SEPARATORS:
        try:
            df = pd.read_csv(StringIO(text), sep=sep, dtype=str)
            break
        except Exception:
            df = None

    if df is None:
        # HTML fallback
        tables = pd.read_html(text, header=0)
        if not tables:
            raise ValueError("No table found in response.")
        df = tables[0].astype(str)

    # Trim strings & clean column names
    df = df.astype(str)
    for c in df.columns:
        df[c] = df[c].str.strip()
    df.columns = [clean_colname(c) for c in df.columns]
    return df

def find_business_key(pdf: pd.DataFrame, table_name: str, candidates=None) -> str | None:
    """Find a key column by common patterns or provided candidates; return the column name or None."""
    cols = list(pdf.columns)
    lowered = {c.lower(): c for c in cols}

    patterns = []
    # common master id variants
    patterns += ["masterid", "master_id", "master_id_", "masterid_"]
    # provided candidates
    if candidates:
        patterns += [str(c).lower() for c in candidates]
    # table-prefixed variants
    tn = table_name.lower()
    patterns += [f"{tn}_masterid", f"{tn}_master_id", f"{tn}masterid", f"{tn}master_id"]

    for p in patterns:
        if p in lowered:
            return lowered[p]

    # direct exacts (case variations)
    for c in cols:
        if c.lower().replace("_","") == "masterid":
            return c

    return None

def to_spark(pdf: pd.DataFrame):
    """Create a Spark DF with string types (safe)."""
    return spark.createDataFrame(pdf)

def append_only_by_key(df_new, table_name: str, key_col: str):
    """Append only rows whose key is not in target. Create table if it doesn't exist."""
    if spark.catalog.tableExists(table_name):
        existing_keys = spark.table(table_name).select(key_col).distinct()
        new_rows = df_new.join(existing_keys, on=key_col, how="left_anti").dropDuplicates([key_col])
        if new_rows.head(1):
            new_rows.writeTo(table_name).append()
            print(f"✅ [{table_name}] appended {new_rows.count()} new rows (key={key_col}).")
        else:
            print(f"✅ [{table_name}] no new rows to append (key={key_col}).")
    else:
        base = df_new.dropDuplicates([key_col])
        base.writeTo(table_name).using("delta").create()
        print(f"✅ [{table_name}] created with {base.count()} rows (full load, key={key_col}).")

def append_only_by_rowhash(df_new, table_name: str):
    """If no natural key: build a stable row hash across all columns and append only new hashes."""
    # Build a deterministic hash for each row based on all columns as strings
    cols = df_new.columns
    concat_all = F.concat_ws("␟", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols])  # rare delimiter
    df_hashed = df_new.withColumn("_ingest_hash", F.sha2(concat_all, 256))

    if spark.catalog.tableExists(table_name):
        existing_hashes = spark.table(table_name).select("_ingest_hash").distinct()
        new_rows = df_hashed.join(existing_hashes, on="_ingest_hash", how="left_anti").dropDuplicates(["_ingest_hash"])
        if new_rows.head(1):
            new_rows.writeTo(table_name).append()
            print(f"✅ [{table_name}] appended {new_rows.count()} new rows (by row hash).")
        else:
            print(f"✅ [{table_name}] no new rows to append (by row hash).")
    else:
        base = df_hashed.dropDuplicates(["_ingest_hash"])
        base.writeTo(table_name).using("delta").create()
        print(f"✅ [{table_name}] created with {base.count()} rows (full load, by row hash).")

# ---------- main loop ----------
for src in SOURCES:
    name = src["name"]
    url  = src["url"]
    auth = tuple(src["auth"]) if src.get("auth") else None
    cand = src.get("key_candidates")

    print(f"\n==== Loading: {name} ====")
    try:
        pdf = read_table_like(url, auth=auth)
        key = find_business_key(pdf, table_name=name, candidates=cand)

        # Drop blank rows on the chosen key (if any)
        if key:
            pdf[key] = pdf[key].astype(str).str.strip()
            pdf = pdf[pdf[key].notna() & (pdf[key] != "")]
            df_new = to_spark(pdf)
            append_only_by_key(df_new, table_name=name, key_col=key)
        else:
            print(f"ℹ️  No business key found for '{name}'. Using row-hash to avoid duplicates.")
            df_new = to_spark(pdf)
            append_only_by_rowhash(df_new, table_name=name)

    except requests.exceptions.RequestException as e:
        print(f"❌ [{name}] network error: {e}")
    except Exception as e:
        print(f"❌ [{name}] error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ========= keep your SOURCES and CSV_SEPARATORS as-is =========
SOURCES = [
    {
        "name": "Buchungen_Basis",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obumvsggnrqmuwtimrwmuwtizdegqwweodgmywtaztcgzswenjrmqydomy/Buchungen_Basis.html",
        "auth": ("test", "test"),
        "key_candidates": "MasterID",
    },
    {
        "name": "Buchungen_Basis_Budget",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6mrtgvsdsn3bmiwtmndgguwtizjrg4wwcmlegywwczbrgzrwknjvhbtdcyy/Buchungen_Basis_Budget.html",
        "auth":  ("test", "test")
    },
    
    {
        "name": "BWA_Gliederung",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6m3cgiygczlbg4wtgztdhewtimzwmiwtsytfhewtmojvguytonjymyytkoa/BWA_Gliederung.html",
        "auth": ("test", "test")
        # no key_candidates -> will auto-detect; if none found, will hash row
    },
    
    {
        "name": "Kontakte",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obvgfrgcnlggywweodfhawtiyzvgewtqmzwgqwtinrqmnrtsmlbgazwkma/Kontakte.html",
        "auth": ("test", "test"),
        "key_candidates": "master_id"
    },
    
    {
        "name": "Kreditoren",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6nztgqydomrsg4wtsntgmywtinzwgawtsnbygqwtcmrqgfsdkmzwgntgkzq/Kreditoren.html",
        "auth": ("test", "test"),
        "key_candidates": "Kontakt_MasterID",
    },
    
    {
        "name": "Kreditoren_Gesamtumsatz",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6mbugbsdknjymywtiyjvgewtimbwguwtqojxmiwtcmbtmu3wmzlemqzdmzq/Kreditoren_Gesamtumsatz_.html",
        "auth":("test", "test"),
         "key_candidates": "masterid",
        
        
    },
    
    {
        "name": "OP_Liste",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6y3emeytombxgqwwiyleguwtizbwmywwenrxmuwtczjrmy3tenbxguygimi/OP_Liste.html",
        "auth": ("test", "test"),
         "key_candidates": "masterid",
    },
    
    {
        "name": "SCRBWA_Gliederung",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6zbygfrdgmrqmiwtcmrtmuwtizbthewwcmrqgqwwcndbmiydmytfgy4doma/SCRBWA_Gliederung.html",
        "auth":  ("test", "test"),
         #no key_candidates -> will auto-detect; if none found, will hash row
    }
]

CSV_SEPARATORS = [";", ","]  # try in order, then fallback to HTML
# =================================================

# (omitted here for brevity)

spark = SparkSession.getActiveSession() or SparkSession.builder.appName("Multi-Incremental-Loader").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# ---------- helpers ----------
def clean_colname(name: str) -> str:
    name = (name or "").strip().replace(" ", "_")
    return "".join(ch if (ch.isalnum() or ch == "_") else "" for ch in name)

def make_unique(names):
    seen, out = {}, []
    for n in names:
        base = n
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
    return out

def read_table_like(url: str, auth=None) -> pd.DataFrame:
    resp = requests.get(url, auth=auth, timeout=90)
    resp.raise_for_status()
    text = resp.text

    df = None
    for sep in CSV_SEPARATORS:
        try:
            df = pd.read_csv(StringIO(text), sep=sep, dtype=str)
            break
        except Exception:
            df = None
    if df is None:
        tables = pd.read_html(text, header=0)
        if not tables:
            raise ValueError("No table found in response.")
        df = tables[0].astype(str)

    df = df.astype(str)
    for c in df.columns:
        df[c] = df[c].str.strip()
    df.columns = [clean_colname(c) for c in df.columns]
    df.columns = make_unique(df.columns)
    return df

def normalize_candidates(cand):
    if not cand: return []
    if isinstance(cand, str): return [cand.lower()]
    return [str(x).lower() for x in cand]

def find_business_key(pdf: pd.DataFrame, table_name: str, candidates=None) -> str | None:
    cols = list(pdf.columns)
    lowered = {c.lower(): c for c in cols}
    patterns = ["masterid", "master_id"] + normalize_candidates(candidates)
    tn = table_name.lower()
    patterns += [f"{tn}_masterid", f"{tn}_master_id", f"{tn}masterid", f"{tn}master_id"]
    for p in patterns:
        if p in lowered:
            return lowered[p]
    for c in cols:
        if c.lower().replace("_","") == "masterid":
            return c
    return None

def to_spark(pdf: pd.DataFrame):
    return spark.createDataFrame(pdf)

def align_to_target_schema(df_new, df_target):
    """Cast/align df_new to df_target schema; select exactly target columns (order preserved)."""
    tfields = df_target.schema.fields
    for f in tfields:
        if f.name in df_new.columns:
            # cast if types differ
            if df_new.schema[f.name].dataType != f.dataType:
                df_new = df_new.withColumn(f.name, F.col(f.name).cast(f.dataType))
        else:
            # add missing column as null cast to target type
            df_new = df_new.withColumn(f.name, F.lit(None).cast(f.dataType))
    # select only target columns, in order
    df_new = df_new.select([f.name for f in tfields])
    return df_new

def append_only_by_key(df_new, table_name: str, key_col: str):
    src_count = df_new.count()
    if spark.catalog.tableExists(table_name):
        df_target = spark.table(table_name)
        existing_keys = df_target.select(key_col).distinct()
        new_rows_pre = df_new.join(existing_keys, on=key_col, how="left_anti").dropDuplicates([key_col])
        # align schema to avoid Delta “merge fields” errors
        new_rows = align_to_target_schema(new_rows_pre, df_target)
        new_count = new_rows.count()
        if new_count > 0:
            new_rows.writeTo(table_name).append()
        total_after = spark.table(table_name).count()
        print(f"✅ [{table_name}] source={src_count}, appended={new_count}, total_after={total_after} (key={key_col}).")
    else:
        base = df_new.dropDuplicates([key_col])
        base.writeTo(table_name).using("delta").create()
        total_after = spark.table(table_name).count()
        print(f"✅ [{table_name}] CREATED: source={src_count}, written={total_after} (full load, key={key_col}).")

def append_only_by_rowhash(df_new, table_name: str):
    src_count = df_new.count()
    if spark.catalog.tableExists(table_name):
        df_target = spark.table(table_name)
        # hash on common columns to be schema-safe
        common_cols = [c for c in df_new.columns if c in df_target.columns] or df_new.columns

        def with_hash(df, cols):
            concat_all = F.concat_ws("\u241F", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols])
            return df.select("*", F.sha2(concat_all, 256).alias("_rowhash"))

        new_h   = with_hash(df_new,   common_cols)
        exist_h = with_hash(df_target, common_cols)

        delta_pre = new_h.join(exist_h.select("_rowhash"), on="_rowhash", how="left_anti").drop("_rowhash")
        # align schema to target before append
        delta_rows = align_to_target_schema(delta_pre, df_target)
        new_count = delta_rows.count()
        if new_count > 0:
            delta_rows.writeTo(table_name).append()
        total_after = spark.table(table_name).count()
        print(f"✅ [{table_name}] source={src_count}, appended={new_count}, total_after={total_after} (row-hash on {len(common_cols)} cols).")
    else:
        # first run: write as-is
        df_new.writeTo(table_name).using("delta").create()
        total_after = spark.table(table_name).count()
        print(f"✅ [{table_name}] CREATED: source={src_count}, written={total_after} (full load, row-hash).")

# ---------- main loop ----------
for src in SOURCES:
    name = src["name"]; url = src["url"]
    auth = tuple(src["auth"]) if src.get("auth") else None
    cand = src.get("key_candidates")

    print(f"\n==== Loading: {name} ====")
    try:
        pdf = read_table_like(url, auth=auth)
        key = find_business_key(pdf, table_name=name, candidates=cand)
        df_new = to_spark(pdf)

        if key:
            # remove blanks on key
            df_new = df_new.filter(F.col(key).isNotNull() & (F.trim(F.col(key)) != ""))
            append_only_by_key(df_new, table_name=name, key_col=key)
        else:
            print(f"ℹ️  No business key found for '{name}'. Using row-hash.")
            append_only_by_rowhash(df_new, table_name=name)

    except requests.exceptions.RequestException as e:
        print(f"❌ [{name}] network error: {e}")
    except Exception as e:
        print(f"❌ [{name}] error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
