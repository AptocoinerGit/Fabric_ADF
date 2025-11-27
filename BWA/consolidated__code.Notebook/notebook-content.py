# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5a316f20-5fd6-483b-92bc-9150832a1348",
# META       "default_lakehouse_name": "consolidated_LH",
# META       "default_lakehouse_workspace_id": "273e76c4-5546-4215-8ae9-07e4e03a2182",
# META       "known_lakehouses": [
# META         {
# META           "id": "5a316f20-5fd6-483b-92bc-9150832a1348"
# META         }
# META       ]
# META     }
# META   }
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


spark = SparkSession.getActiveSession() or SparkSession.builder.appName("Multi-Incremental-Loader").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

CSV_SEPARATORS = [";", ","]

# ===============  HELPERS ===============

def make_unique(cols):
    """Ensure all column names are unique (lowercase) just like your single-link script."""
    seen = {}
    new_cols = []
    for c in cols:
        c = c.lower().strip()
        if c in seen:
            seen[c] += 1
            new_cols.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            new_cols.append(c)
    return new_cols

def read_table_like(url: str, auth=None) -> pd.DataFrame:
    """Read from URL (CSV/HTML), clean columns, lowercase, deduplicate, and cast to string."""
    resp = requests.get(url, auth=auth, timeout=90)
    resp.raise_for_status()
    text = resp.text

    df = None
    for sep in CSV_SEPARATORS:
        try:
            df = pd.read_csv(StringIO(text), sep=sep, encoding="latin1", dtype=str)
            break
        except Exception:
            df = None

    if df is None:
        df = pd.read_html(text, header=0, decimal=',', thousands='.')[0].astype(str)

    # ✅ Clean column names (lowercase, safe chars)
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.replace(" ", "_")
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
        .str.lower()
    )

    # ✅ Make columns unique
    df.columns = make_unique(df.columns)

    # ✅ Convert all data to string (prevents datatype conflicts)
    df = df.astype(str)

    # ✅ Trim all strings
    for c in df.columns:
        df[c] = df[c].str.strip()

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
