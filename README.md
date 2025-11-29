# Crime Data Star Schema and Reports with PySpark

## Project Overview

This project demonstrates an ETL (Extract, Transform, Load) pipeline that processes raw crime data using **PySpark**. The goal is to transform the raw data into a **Star Schema** optimized for analysis and to generate various analytical reports.

The project reads raw crime data from a CSV file, processes it to create dimension and fact tables, saves these tables as Parquet files, and generates CSV reports providing insights into crime statistics.

## Project Structure

```
c:\dev\aueb\spark-project\
├── Data\                   # Data directory (Input CSVs and Output Parquet/Reports)
├── Notebooks\              # Jupyter Notebooks for exploration and schema design
│   ├── check_raw_data.ipynb
│   ├── schema.ipynb        # Main notebook demonstrating the schema creation
│   └── spark_test_connection.ipynb
├── Scripts\                # Python scripts for the ETL process
│   ├── schema_etl.py       # Main ETL script
│   ├── pyspark.txt
│   ├── scala.txt
│   └── test_spark_script.py
├── Project1_Part_Time.pdf  # Project requirements/description
├── requirements.txt        # Python dependencies
└── README.md               # This file
```

## Prerequisites

- **Python 3.x**
- **Java 8 or later** (Required for Spark)
- **Apache Spark** (PySpark)
- **Hadoop (winutils.exe)**: If running on Windows, ensure you have `winutils.exe` and `HADOOP_HOME` environment variable set.

## Setup and Installation

1.  **Clone the repository** (or navigate to the project directory).

2.  **Install Python dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
    *Note: Main dependency is `pyspark`.*

3.  **Environment Setup (Windows)**:
    - Ensure `JAVA_HOME` is set to your Java installation.
    - Ensure `HADOOP_HOME` is set. The script `Scripts/schema_etl.py` attempts to set a default if not present, but it's recommended to configure it manually.

## Usage

### Running the ETL Script

The main ETL logic is encapsulated in `Scripts/schema_etl.py`. You can run it from the command line.

```bash
cd Scripts
python schema_etl.py
```

**Arguments:**
- `--input`: Path to the input CSV file (default: `../Data/Input/CrimeData.csv`).
- `--output`: Path to the output directory (default: `../Data/Output`).

Example:
```bash
python schema_etl.py --input "C:\path\to\data.csv" --output "C:\path\to\output"
```

### Jupyter Notebooks

You can also explore the logic and data using the provided Jupyter Notebooks in the `Notebooks/` directory.
- `schema.ipynb`: Step-by-step walkthrough of the data loading, transformation, and schema creation.

## ETL Process

1.  **Extract**: Reads raw crime data from a CSV file (semicolon separated).
2.  **Transform**:
    -   **Date Normalization**: Converts `DateOccured` to a Date object and generates a `DateKey`.
    -   **Dimension Creation**: Extracts unique values for dimensions:
        -   `dim_date`: Date attributes (Year, Month, Day, DayOfWeek, etc.).
        -   `dim_area`: Area codes and names.
        -   `dim_crime`: Crime codes and descriptions.
        -   `dim_premis`: Premise codes and descriptions.
        -   `dim_status`: Case status codes and descriptions.
        -   `dim_weapon`: Weapon codes and descriptions.
        -   `dim_descent`: Victim descent codes and descriptions.
    -   **Fact Table Creation**: Creates `fact_cases` containing foreign keys to dimensions and measures/attributes (CaseID, VictimAge, VictimSex, Location, etc.).
3.  **Load**: Saves the dimension and fact tables as **Parquet** files in the output directory.

## Reports Generated

The script generates the following reports (saved as CSV in `Data/Output/reports/`):

1.  **Incidents per Area and Premis Type** (`case_per_area_premis`): Counts of incidents grouped by Area and Premise type.
2.  **Top 10 Crimes** (`top10_crimes`): The 10 most frequent crime types.
3.  **Cases per Year/Month** (`cases_per_year`): Time series count of cases.
4.  **Crime Cases Status** (`crime_cases_status`): Breakdown of cases by crime type and status.
5.  **Victim Sex/Age Statistics** (`victim_sex_age`): Aggregated counts based on victim descent, sex, and age.
