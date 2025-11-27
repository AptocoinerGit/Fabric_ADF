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
url = "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6y3emeytombxgqwwiyleguwtizbwmywwenrxmuwtczjrmy3tenbxguygimi/OP_Liste.html"
username = "test"
password = "test"

# ✅ Step 2: Download the data
response = requests.get(url, auth=(username, password))

if response.status_code == 200:
    print(" Successfully downloaded the file")

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
    spark.sql("DROP TABLE IF EXISTS OP_Liste")

    # ✅ Step 7: Save to Lakehouse
    spark.createDataFrame(df).write.mode("overwrite").saveAsTable("OP_Liste")

    print(" Data successfully written to Lakehouse table 'OP_Liste'")

else:
    print(f" Download failed: {response.status_code}")
    print(response.text[:500])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
