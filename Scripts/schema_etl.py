'''
ETL script to transform raw crime CSV data into a star schema and generate reports using PySpark.

Author: Alexandros Kazos
'''

import os
import sys
import argparse
import logging
from typing import Dict
from pyspark.sql import SparkSession, DataFrame, functions as F

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def ensure_windows_hadoop_home():
    """On Windows set a default HADOOP_HOME if unset to avoid known Spark write errors.
    Note: placing a valid winutils.exe under HADOOP_HOME\bin is still required for some Spark versions."""
    if os.name == "nt":
        if not os.environ.get("HADOOP_HOME") and not os.environ.get("hadoop.home.dir"):
            default = r"C:\hadoop"
            os.environ["HADOOP_HOME"] = default
            os.environ["hadoop.home.dir"] = default
            logging.warning(
                "HADOOP_HOME and hadoop.home.dir were not set. Defaulting to %s. "
                "If you see FileNotFound errors, download winutils.exe and set HADOOP_HOME accordingly.", default
            )


def create_spark(app_name: str = "CrimeStarSchema") -> SparkSession:
    ensure_windows_hadoop_home()
    spark = SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()
    logging.info("Spark session created: %s", spark.sparkContext.appName)
    return spark


def load_raw_csv(spark: SparkSession, input_path: str) -> DataFrame:
    logging.info("Reading CSV from %s", input_path)
    return spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv(input_path)


def build_dimensions_and_fact(raw: DataFrame) -> (Dict[str, DataFrame], DataFrame):
    # normalize date and add DateKey
    raw2 = raw.withColumn("Date", F.to_date(F.col("DateOccured"))).withColumn(
        "DateKey", F.date_format(F.col("Date"), "yyyyMMdd").cast("int")
    )

    date_df = (
        raw2.select("Date", "DateKey").dropDuplicates()
        .withColumn("Year", F.year("Date"))
        .withColumn("Month", F.month("Date"))
        .withColumn("Day", F.dayofmonth("Date"))
        .withColumn("DayOfWeek", F.date_format("Date", "EEEE"))
        .withColumn("MonthName", F.date_format("Date", "MMMM"))
        .withColumn("Quarter", F.quarter("Date"))
        .orderBy("DateKey")
    )

    area_df = raw2.select("AreaCode", "Area").dropDuplicates().orderBy("AreaCode")
    crime_df = raw2.select("CrimeCode", "CrimeDescription").dropDuplicates().orderBy("CrimeCode")
    premis_df = raw2.select("PremisCode", "PremisDescription").dropDuplicates().orderBy("PremisCode")
    status_df = raw2.select("CaseStatusCode", "CaseStatusDescription").dropDuplicates().orderBy("CaseStatusCode")
    weapon_df = raw2.select("WeaponCode", "Weapon").dropDuplicates().orderBy("WeaponCode")
    descent_df = raw2.select("VictimDescentCode", "VictimDescent").dropDuplicates().orderBy("VictimDescentCode")

    cases_df = raw2.select(
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
        "longitude",
    )

    tables = {
        "dim_date": date_df,
        "dim_area": area_df,
        "dim_crime": crime_df,
        "dim_premis": premis_df,
        "dim_status": status_df,
        "dim_weapon": weapon_df,
        "dim_descent": descent_df,
        "fact_cases": cases_df,
    }

    return tables, cases_df


def save_parquet_tables(tables: Dict[str, DataFrame], output_base: str):
    os.makedirs(output_base, exist_ok=True)
    for name, df in tables.items():
        out_path = os.path.join(output_base, name)
        logging.info("Writing Parquet: %s", out_path)
        try:
            df.write.mode("overwrite").parquet(out_path)
        except Exception as e:
            logging.error("Failed to write Parquet for %s: %s", name, e)
            raise


def build_reports(tables: Dict[str, DataFrame], cases_df: DataFrame) -> Dict[str, DataFrame]:
    date_df = tables["dim_date"]
    area_df = tables["dim_area"]
    crime_df = tables["dim_crime"]
    premis_df = tables["dim_premis"]
    status_df = tables["dim_status"]
    descent_df = tables["dim_descent"]

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
        .select(F.col("CrimeDescription").alias("Είδος_Εγκλήματος"), F.col("Αριθμός_Περιστατικών"))
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
            F.col("Αριθμός_Περιστατικών"),
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
            F.col("Αριθμός_Περιστατικών"),
        )
        .orderBy(F.asc("Χώρα_Καταγωγής"), F.asc("Φύλο"), F.asc("Ηλικία"))
    )

    return {
        "case_per_area_premis": case_per_area_premis,
        "top10_crimes": top10_crimes,
        "cases_per_year": cases_per_year,
        "crime_cases_status": crime_cases_status,
        "victim_sex_age": victim_sex_age,
    }


def save_reports(reports: Dict[str, DataFrame], output_base: str):
    reports_dir = os.path.join(output_base, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    for name, df in reports.items():
        out = os.path.join(reports_dir, name)
        logging.info("Writing report CSV: %s", out)
        try:
            df.coalesce(1).write.csv(out, mode="overwrite", header=True)
            logging.info("Saved report: %s", name)
        except Exception as e:
            logging.error("Failed to save report %s: %s", name, e)
            raise


def parse_args():
    p = argparse.ArgumentParser(description="Transform raw crime CSV into star schema and reports with PySpark")
    p.add_argument("--input", default=os.path.join("..", "Data", "Input", "CrimeData.csv"))
    p.add_argument("--output", default=os.path.join("..", "Data", "Output"))
    return p.parse_args()


def main():
    args = parse_args()
    spark = create_spark()
    try:
        raw = load_raw_csv(spark, args.input)
        tables, cases_df = build_dimensions_and_fact(raw)
        save_parquet_tables(tables, args.output)
        reports = build_reports(tables, cases_df)
        save_reports(reports, args.output)
    finally:
        spark.stop()
        logging.info("Spark stopped.")


if __name__ == "__main__":
    main()
