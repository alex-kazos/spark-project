#%% md
# ## Imports
#%%
import os
from pyspark.sql import SparkSession, functions as F
#%% md
# ## Spark Session
#%%
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("CrimeStarSchema")
    .getOrCreate()
)
#%% md
# ## Load Data
#%%
input_path = os.path.join("..", "Data", "Input", "CrimeData.csv")
output_base = os.path.join("..", "Data", "Output")
#%%
# Read raw CSV (semicolon separator)
raw = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv(input_path)
#%%
raw
#%%
# Normalize dates and add DateKey (yyyymmdd int)
raw = (
    raw.withColumn("Date", F.to_date(F.col("DateOccured")))
         .withColumn("DateKey", F.date_format(F.col("Date"), "yyyyMMdd").cast("int"))
)
#%%
raw.show(5)
#%% md
# ---
# ## Dimensions and Fact Tables
# 
# ![image.png](attachment:f78467fc-cc5b-41e0-b80b-78e5ba87f013.png)
# 
# _Note: for more information regarding the proposed star schema please refer to the check_raw_data notebook._
#%% md
# ## Dimensions
#%% md
# #### 1) Date dimension
#%%
date_df = (
    raw.select("Date", "DateKey").dropDuplicates()
        .withColumn("Year", F.year("Date"))
        .withColumn("Month", F.month("Date"))
        .withColumn("Day", F.dayofmonth("Date"))
        .withColumn("DayOfWeek", F.date_format("Date", "EEEE"))
        .withColumn("MonthName", F.date_format("Date", "MMMM"))
        .withColumn("Quarter", F.quarter("Date"))
        .orderBy("DateKey")
)
#%%
date_df.show(5)
#%% md
# #### 2) Area dimension
#%%
area_df = raw.select("AreaCode", "Area").dropDuplicates().orderBy("AreaCode")
#%%
area_df.show(5)
#%% md
# #### 3) Crime dimension
#%%
crime_df = raw.select("CrimeCode", "CrimeDescription").dropDuplicates().orderBy("CrimeCode")
#%%
crime_df.show(5)
#%% md
# #### 4) Premis dimension
#%%
premis_df = raw.select("PremisCode", "PremisDescription").dropDuplicates().orderBy("PremisCode")
#%%
premis_df.show(5)
#%% md
# #### 5) Status dimension
#%%
status_df = raw.select("CaseStatusCode", "CaseStatusDescription").dropDuplicates().orderBy("CaseStatusCode")
#%%
status_df.show(5)
#%% md
# #### 6) Weapon dimension
#%%
weapon_df = raw.select("WeaponCode", "Weapon").dropDuplicates().orderBy("WeaponCode")
#%%
weapon_df.show(5)
#%% md
# #### 7) Descent dimension
#%%
descent_df = raw.select("VictimDescentCode", "VictimDescent").dropDuplicates().orderBy("VictimDescentCode")
#%%
descent_df.show(5)
#%% md
# ---
# ### Fact table
# Cases (keep CaseID + foreign keys + attributes)
#%%
cases_df = raw.select(
    "CaseID",
    "DateKey",
    "AreaCode",
    "CrimeCode",
    "VictimAge",
    "VictimSex",
    "VictimDescentCode",
    "PremisCode",
    "WeaponCode",
    "CaseStatusCode",
    "latitude",
    "longitude"
)
#%%
cases_df.show(5)
#%% md
# ### Register as tables as views
#%%
date_df.createOrReplaceTempView("dim_date")
area_df.createOrReplaceTempView("dim_area")
crime_df.createOrReplaceTempView("dim_crime")
premis_df.createOrReplaceTempView("dim_premis")
status_df.createOrReplaceTempView("dim_status")
weapon_df.createOrReplaceTempView("dim_weapon")
descent_df.createOrReplaceTempView("dim_descent")
cases_df.createOrReplaceTempView("fact_cases")
#%% md
# ### Save as Parquet
#%%
# Write dimensions and fact to Parquet
tables = {
    "dim_date": date_df,
    "dim_area": area_df,
    "dim_crime": crime_df,
    "dim_premis": premis_df,
    "dim_status": status_df,
    "dim_weapon": weapon_df,
    "dim_descent": descent_df,
    "fact_cases": cases_df
}

for name, df in tables.items():
    out_path = os.path.join(output_base, name)
    df.write.mode("overwrite").parquet(out_path)
#%% md
# ---
# 
# ## Reports
#%% md
# ### Incidents per Area and Premis Type
#%%
case_per_area_premis_df = (
    cases_df.alias("f")
        .join(area_df.alias("a"), F.col("f.AreaCode") == F.col("a.AreaCode"), "left")
        .join(premis_df.alias("p"), F.col("f.PremisCode") == F.col("p.PremisCode"), "left")
        .groupBy("a.Area", "p.PremisDescription")
        .agg(F.count("*").alias("Αριθμός_Περιστατικών"))
        .select(F.col("Area"), F.col("PremisDescription").alias("Τύπος"), F.col("Αριθμός_Περιστατικών"))
        .orderBy(F.asc("Area"), F.desc("Αριθμός_Περιστατικών"))
)
#%%
case_per_area_premis_df.show(5)
#%% md
# ### Top 10 Crimes
#%%
top10_crimes = (
    cases_df.alias("f")
        .join(crime_df.alias("c"), F.col("f.CrimeCode") == F.col("c.CrimeCode"), "left")
        .groupBy("c.CrimeDescription")
        .agg(F.count("*").alias("Αριθμός_Περιστατικών"))
        .select(
            F.col("CrimeDescription").alias("Είδος_Εγκλήματος"),
            F.col("Αριθμός_Περιστατικών")
        )
        .orderBy(F.desc("Αριθμός_Περιστατικών"))
        .limit(10)
)

#%%
top10_crimes.show(5)
#%% md
# ### Monthly Incident Count by Year
#%%
cases_per_year = (
    cases_df.alias("f")
        .join(date_df.alias("d"), F.col("f.DateKey") == F.col("d.DateKey"), "left")
        .groupBy("d.Year", "d.Month")
        .agg(F.count("*").alias("Αριθμός_Περιστατικών"))
        .orderBy(F.asc("Year"), F.asc("Month"))
)
#%%
cases_per_year.show(5)
#%% md
# ### Incident Status by Crime Type
#%%
crime_cases_status = (
    cases_df.alias("f")
        .join(crime_df.alias("c"), F.col("f.CrimeCode") == F.col("c.CrimeCode"), "left")
        .join(status_df.alias("s"), F.col("f.CaseStatusCode") == F.col("s.CaseStatusCode"), "left")
        .groupBy("c.CrimeDescription", "s.CaseStatusDescription")
        .agg(F.count("*").alias("Αριθμός_Περιστατικών"))
        .select(
            F.col("CrimeDescription").alias("Είδος_Εγκλήματος"),
            F.col("CaseStatusDescription").alias("Κατάσταση_Περιστατικού"),
            F.col("Αριθμός_Περιστατικών")
        )
        .orderBy(F.asc("Είδος_Εγκλήματος"), F.asc("Κατάσταση_Περιστατικού"))
)
#%%
crime_cases_status.show(5)
#%% md
# ### Data Cube (Victim Descent, Sex, Age)
#%%
victim_sex_age = (
    cases_df.alias("f")
        .join(descent_df.alias("d"), F.col("f.VictimDescentCode") == F.col("d.VictimDescentCode"), "left")
        .rollup("d.VictimDescent", "f.VictimSex", "f.VictimAge")
        .agg(F.count("*").alias("Αριθμός_Περιστατικών"))
        .na.fill("TOTAL")
        .select(
            F.coalesce(F.col("VictimDescent"), F.lit("Total Descent")).alias("Χώρα_Καταγωγής"),
            F.coalesce(F.col("VictimSex"), F.lit("Total Sex")).alias("Φύλο"),
            F.coalesce(F.col("VictimAge").cast("string"), F.lit("Total Age")).alias("Ηλικία"),
            F.col("Αριθμός_Περιστατικών")
        )
        .orderBy(F.asc("Χώρα_Καταγωγής"), F.asc("Φύλο"), F.asc("Ηλικία"))
)
#%%
victim_sex_age.show(5)
#%% md
# ## Save Reports
#%%
# Save Reports
reports = {
    "case_per_area_premis_df": case_per_area_premis_df,
    "top10_crimes": top10_crimes,
    "cases_per_year": cases_per_year,
    "crime_cases_status": crime_cases_status,
    "victim_sex_age": victim_sex_age
}

for name, df in reports.items():
    # Save each report to a single CSV file
    df.coalesce(1).write.csv(os.path.join(output_base, "reports", name), mode="overwrite", header=True)
    print(f"Saved report: {name}")
#%%
spark.stop()
#%%
