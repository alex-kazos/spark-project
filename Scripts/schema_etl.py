'''
ETL script to transform raw crime CSV data into a star schema and generate reports using PySpark.

Author: Alexandros Kazos
'''

#!/usr/bin/env python3
# python

import os
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="py4j")
import argparse
from pyspark.sql import SparkSession, functions as F


def create_spark(warehouse_path):
    os.makedirs(warehouse_path, exist_ok=True)
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("CrimeStarSchema")
        .config("spark.sql.warehouse.dir", warehouse_path)
        .config("javax.jdo.option.ConnectionURL",
                "jdbc:derby:memory:metastore_db;create=true")
        .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
        .config("hive.metastore.schema.verification", "false")
        .config("spark.sql.hive.thriftServer.singleSession", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    print("Spark session created with in-memory metastore!")
    return spark


def load_raw(spark, input_path):
    raw = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv(input_path)
    raw = (
        raw.withColumn("Date", F.to_date(F.col("DateOccured")))
           .withColumn("DateKey", F.date_format(F.col("Date"), "yyyyMMdd").cast("int"))
    )
    return raw


def build_dimensions(raw):
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

    area_df = raw.select("AreaCode", "Area").dropDuplicates().orderBy("AreaCode")
    crime_df = raw.select("CrimeCode", "CrimeDescription").dropDuplicates().orderBy("CrimeCode")
    premis_df = raw.select("PremisCode", "PremisDescription").dropDuplicates().orderBy("PremisCode")
    status_df = raw.select("CaseStatusCode", "CaseStatusDescription").dropDuplicates().orderBy("CaseStatusCode")
    weapon_df = raw.select("WeaponCode", "Weapon").dropDuplicates().orderBy("WeaponCode")
    descent_df = raw.select("VictimDescentCode", "VictimDescent").dropDuplicates().orderBy("VictimDescentCode")

    return {
        "dim_date": date_df,
        "dim_area": area_df,
        "dim_crime": crime_df,
        "dim_premis": premis_df,
        "dim_status": status_df,
        "dim_weapon": weapon_df,
        "dim_descent": descent_df,
    }


def build_fact(raw):
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
    return {"fact_cases": cases_df}


def register_and_save_tables(tables, output_base):
    for table_name, df in tables.items():
        df.createOrReplaceGlobalTempView(table_name)
        print(f"Registered table: {table_name}")

    for name, df in tables.items():
        out_dir = os.path.join(output_base, "Schema" if name.startswith("dim_") or name.startswith("fact_") else "Reports", name)
        df.coalesce(1).write.csv(out_dir, mode="overwrite", header=True)
        print(f"Saved Table: {name} -> {out_dir}")


def build_reports(cases_df, dims):
    date_df = dims["dim_date"]
    area_df = dims["dim_area"]
    crime_df = dims["dim_crime"]
    premis_df = dims["dim_premis"]
    status_df = dims["dim_status"]
    descent_df = dims["dim_descent"]

    case_per_area_premis = (
        cases_df.alias("f")
            .join(area_df.alias("a"), F.col("f.AreaCode") == F.col("a.AreaCode"), "left")
            .join(premis_df.alias("p"), F.col("f.PremisCode") == F.col("p.PremisCode"), "left")
            .groupBy("a.Area", "p.PremisDescription")
            .agg(F.count("*").alias("Αριθμός_Περιστατικών"))
            .select(F.col("Area"), F.col("PremisDescription").alias("Τύπος"), F.col("Αριθμός_Περιστατικών"))
            .orderBy(F.asc("Area"), F.desc("Αριθμός_Περιστατικών"))
    )

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

    cases_per_year = (
        cases_df.alias("f")
            .join(date_df.alias("d"), F.col("f.DateKey") == F.col("d.DateKey"), "left")
            .groupBy("d.Year", "d.Month")
            .agg(F.count("*").alias("Αριθμός_Περιστατικών"))
            .orderBy(F.asc("Year"), F.asc("Month"))
    )

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

    reports = {
        "case_per_area_premis": case_per_area_premis,
        "top10_crimes": top10_crimes,
        "cases_per_year": cases_per_year,
        "crime_cases_status": crime_cases_status,
        "victim_sex_age": victim_sex_age
    }

    return reports


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep-session", action="store_true", help="Do not stop Spark session at end")
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, ".."))
    warehouse_path = os.path.join(project_root, "Data", "Output", "spark-warehouse")
    input_path = os.path.join(project_root, "Data", "Input", "CrimeData.csv")
    output_base = os.path.join(project_root, "Data", "Output")

    spark = create_spark(warehouse_path)

    try:
        raw = load_raw(spark, input_path)
        print("Raw loaded. Sample:")
        raw.show(5)

        dims = build_dimensions(raw)
        fact = build_fact(raw)
        schema_tables = {**dims, **fact}

        # register and save schema tables
        for table_name, df in schema_tables.items():
            df.createOrReplaceGlobalTempView(table_name)
            print(f"Registered table: {table_name}")

        schema_out = os.path.join(output_base, "Schema")
        os.makedirs(schema_out, exist_ok=True)
        for name, df in schema_tables.items():
            out_dir = os.path.join(schema_out, name)
            df.coalesce(1).write.csv(out_dir, mode="overwrite", header=True)
            print(f"Saved Table: {name} -> {out_dir}")

        # build and save reports
        reports = build_reports(fact["fact_cases"], dims)
        reports_out = os.path.join(output_base, "Reports")
        os.makedirs(reports_out, exist_ok=True)
        for name, df in reports.items():
            df.createOrReplaceGlobalTempView(name)
            out_dir = os.path.join(reports_out, name)
            df.coalesce(1).write.csv(out_dir, mode="overwrite", header=True)
            print(f"Saved report: {name} -> {out_dir}")

    finally:
        if not args.keep_session:
            spark.stop()
            print("Spark session stopped.")


if __name__ == "__main__":
    main()
