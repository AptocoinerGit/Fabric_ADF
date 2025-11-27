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

# Use existing spark session
try:
    print(f"Using existing Spark session: {spark.version}")
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("HTML Table to Lakehouse") \
        .getOrCreate()

url = "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6nztgqydomrsg4wtsntgmywtinzwgawtsnbygqwtcmrqgfsdkmzwgntgkzq/Kreditoren.html"
username = "test"
password = "test"

try:
    response = requests.get(url, auth=(username, password), timeout=30)
    
    if response.status_code == 200:
        print("✅ Successfully downloaded the file")
        
        # Try CSV first (most likely format based on your success)
        try:
            df = pd.read_csv(StringIO(response.text), sep=';', encoding='latin1')
            print("Parsed as CSV with semicolon separator")
        except Exception as e:
            print(f"CSV parsing failed: {e}")
            print("Trying as HTML table instead...")
            df = pd.read_html(response.text, header=0)[0]
            print("Parsed as HTML table")

        # Handle duplicate column names by appending a suffix
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
        df.columns = cols

        # Clean column names
        df.columns = (
            df.columns.astype(str).str.strip()
            .str.replace(" ", "_")
            .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
        )

        # Handle European number format if applicable
        numeric_cols = ["Soll", "Haben", "Betrag"]  # Adjust based on actual column names
        for col in numeric_cols:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(".", "", regex=False)
                    .str.replace(",", ".", regex=False)
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")

        print(f"Data shape: {df.shape}")
        print("Sample ")
        print(df.head())

        # Convert to Spark DataFrame and save to Lakehouse
        spark_df = spark.createDataFrame(df)
        spark_df.write.mode("overwrite").format("delta").saveAsTable("Kreditoren")
        print("✅ Data successfully saved to Lakehouse table: Kreditoren")
        
    else:
        print(f"❌ Download failed: {response.status_code}")
        print(response.text[:500])

except requests.exceptions.RequestException as e:
    print(f"Network error: {e}")
except Exception as e:
    print(f"Error processing  {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
