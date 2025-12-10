# Databricks notebook source
# Read raw data
raw_data = "/Volumes/sandeep_practice/default/db/main_data.csv"
raw_df = spark.read.csv(raw_data,header=True,inferSchema=True)

def clean_unique(columns):
    check = {}
    same = []
    for i in columns:
        columns_clean = (
            i.strip()
            .replace(" ", "_")
            .replace(",", "")
            .replace("/", "")
            .replace("-", "")
            .replace("(", "")
            .replace(")", "")
            .replace(".", "")
            .replace("'", "")
            .replace('"', "")
            .replace(":", "")
            .replace(";", "")
            .replace("?", "")
            .replace("!", "")
        )
        if columns_clean not in check:
            check[columns_clean] = 1
            same.append(columns_clean)
        else:
            check[columns_clean] += 1
            same.append(columns_clean + "_" + str(check[columns_clean]))
    return same

columns_clean = clean_unique(raw_df.columns)
bronze_df = raw_df.toDF(*columns_clean)

# Save cleaned data as a managed Delta table
bronze_table = "/Volumes/sandeep_practice/default/db/delta/"
bronze_df.write.format("delta").mode("overwrite").save(bronze_table)

# silver layer Cleansed data
price_col=next((x for x in bronze_df.columns if x.lower().startswith('price')), "Price")
stcol=next((x for x in bronze_df.columns if x.lower().startswith('stock')), "Stock")

silver_df = bronze_df.dropna(subset=[price_col]).filter(f"{stcol} > 0")

# saving silver layer
silver_table = "/Volumes/sandeep_practice/default/db/silver_layer/"
silver_df.write.format("delta").mode("overwrite").save(silver_table)

# golden layer
# performing aggregation
from pyspark.sql import functions as F
gold_df = silver_df.groupBy("Category").agg(F.sum(price_col).alias("Total_Price"))

# storing gold layer
gold_table1 = "/Volumes/sandeep_practice/default/db/golden_layer/"
gold_df.write.format("delta").mode("overwrite").save(gold_table1)

display(gold_df)
