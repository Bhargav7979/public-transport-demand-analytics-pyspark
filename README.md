# Public Transport Passenger Demand Analytics Using Apache Spark and PySpark

> **Academic Course Project**: Big Data Analytics (BDA) Laboratory  
> **Evaluation Tier**: Suitable for a Two-Student Team (15-Mark Laboratory Evaluation)  
> **Primary Technologies**: Python, Apache Spark (PySpark), Streamlit, Pandas, Plotly, Matplotlib  

---

## ⚠️ Academic & Synthetic Dataset Notice

> **IMPORTANT DISCLAIMER**: The passenger data (`data/passenger_data.csv`) provided in this repository is **synthetically generated** for academic instruction, laboratory demonstration, and viva-voce evaluation. It reflects simulated commuter patterns and **must not be cited or represented as official public transportation statistics**.

---

## 📌 Project Overview

Urban transit authorities face significant challenges in matching bus fleet capacity to dynamic commuter demand. Overcrowding occurs during peak morning and evening hours, while midday and late-night routes frequently run with low capacity utilization.

This project delivers an end-to-end Big Data solution that:
1. Ingests and validates multi-route transit telemetry datasets.
2. Employs **Apache Spark (PySpark)** for distributed transformations, aggregations, and metrics computation:
   - Route-wise total and average passenger volumes
   - Commuter peak-hour and time-slot demand identification
   - Daily temporal demand trends
   - Bus fleet capacity utilization and overcrowding classification
3. Provides an interactive, modern **Streamlit web dashboard** featuring real-time KPI metrics, Plotly interactive visualizations, Matplotlib static charts, custom CSV dataset uploads with schema validation, and CSV export capabilities.
4. Includes an optional **Hadoop HDFS architectural module** detailing distributed ingestion and Parquet storage workflows.

---

## 🛠️ Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Big Data Engine** | Apache Spark / PySpark | Distributed DataFrame transformations, aggregations, and pivot operations |
| **User Interface** | Streamlit | Web dashboard, dynamic controls, file uploader, and KPI metric cards |
| **Data Visualization** | Plotly & Matplotlib | Interactive bar charts, heatmaps, pie charts, and academic figures |
| **Data Manipulation** | Pandas & NumPy | In-memory data manipulation, file parsing, and resilient fallback |
| **Storage Layer** | CSV / Optional HDFS | Local structured storage and Hadoop cluster integration interface |

---

## 📁 Project Structure

```text
BDA Project/
│
├── app.py                   # Streamlit web dashboard & visualization interface
├── analysis.py              # PySpark data processing, transformations, and fallback engine
├── generate_data.py         # Reproducible synthetic dataset generator script
├── requirements.txt         # Project dependencies
├── README.md                # Comprehensive project documentation and lab guide
├── .gitignore               # Ignored files (virtualenv, spark-warehouse, caches)
│
└── data/
    └── passenger_data.csv   # Synthetic transit dataset (1,080 records across 30 days)
```

---

## 📊 Dataset Specification

The bundled dataset (`data/passenger_data.csv`) models transit operations across 30 days for 6 major city routes and 6 operating time slots.

| Column Name | Data Type | Description | Example |
|---|---|---|---|
| `Date` | String / Date (`YYYY-MM-DD`) | Date of transit operation | `2026-09-01` |
| `Route` | String | Transit line designation | `Route 101 - Downtown Express` |
| `TimeSlot` | String | Operational operational interval | `08:00 - 10:00 (Morning Peak)` |
| `Passengers` | Integer | Total riders boarding during slot | `703` |
| `BusesOperating` | Integer | Active bus units deployed | `17` |

To regenerate or customize the dataset:
```powershell
python generate_data.py
```

---

## ⚙️ Prerequisites & Environment Setup (Windows & VS Code)

### 1. Python Environment
Ensure Python 3.10 or 3.11 (64-bit) is installed:
```powershell
python --version
```

### 2. Java JDK Configuration (Required for Apache Spark)
Apache Spark runs on the Java Virtual Machine (JVM). It requires a **64-bit Java JDK** (JDK 8, 11, or 17 recommended):
1. Download OpenJDK from [Eclipse Temurin](https://adoptium.net/) or install via Windows terminal:
   ```powershell
   winget install EclipseAdoptium.Temurin.17.JDK
   ```
2. Verify Java installation:
   ```powershell
   java -version
   ```
3. Set your `JAVA_HOME` environment variable (e.g., `C:\Program Files\Eclipse Adoptium\jdk-17.x.x`):
   ```powershell
   [System.Environment]::SetEnvironmentVariable('JAVA_HOME', 'C:\Program Files\Eclipse Adoptium\jdk-17.0.x', 'User')
   ```

> **Resilient Fallback Mode**: If Java is not configured on the evaluation machine, the system will **automatically fall back to Pandas Compatibility Mode**. All KPI cards, route comparisons, and visual charts will remain 100% operational with an informative badge on the dashboard.

---

## 🚀 Installation & Running

### Step 1: Open the Project in VS Code
Open VS Code, press `Ctrl + ~` to open the integrated PowerShell terminal, and ensure you are in the project folder:
```powershell
cd "e:\BDA Project"
```

### Step 2: (Optional) Create and Activate a Virtual Environment
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### Step 3: Install Required Dependencies
```powershell
pip install -r requirements.txt
```

### Step 4: Run the Standalone PySpark Analysis Test
Verify that the analytics pipeline executes properly:
```powershell
python analysis.py
```

### Step 5: Launch the Streamlit Dashboard
```powershell
streamlit run app.py
```
The application will launch automatically in your browser at `http://localhost:8501`.

---

## 🖥️ Streamlit Dashboard Features

1. **Top KPI Metrics Bar**: Total cumulative passengers, total bus trips dispatched, average load per bus, top-demand route, and peak operating slot.
2. **Route Analytics Tab**:
   - Total passenger ridership by route (Plotly horizontal bar chart).
   - Fleet allocation efficiency (average load per bus).
   - Spark summary aggregation table (`groupBy("Route").agg(...)`).
3. **Peak Time Slots Tab**:
   - Commuter distribution across morning peak, evening peak, off-peak, and night intervals.
   - Commuter share donut chart.
   - Matplotlib static bar chart formatted for lab reports.
4. **Daily Trends Tab**:
   - 30-day temporal trend chart correlating daily ridership against available bus capacity.
   - Crowding classification distribution (`High Crowding`, `Optimal Load`, `Low Demand`).
5. **Route × Time Slot Matrix Tab**:
   - Interactive cross-tabulation heatmap generated via PySpark `.pivot()`.
6. **Data Explorer & Upload**:
   - Custom CSV uploader with automated schema validation and missing-value checks.
   - Filtered dataset preview with CSV download button.
7. **Hadoop HDFS Architecture Tab**:
   - Explains cluster deployment, HDFS CLI commands, and distributed Parquet I/O.

---

## 🐘 Optional Hadoop HDFS Module

For students wishing to demonstrate Big Data storage architecture in the lab evaluation, `analysis.py` contains utility functions:

- `export_to_hdfs(spark, df, hdfs_path)`: Writes PySpark DataFrames to HDFS as partitioned Parquet.
- `read_from_hdfs(spark, hdfs_path)`: Reads distributed datasets from HDFS NameNodes.

**Standard HDFS CLI Workflow:**
```bash
# 1. Create HDFS directory
hdfs dfs -mkdir -p /bda_project/data

# 2. Upload transit CSV to HDFS
hdfs dfs -put data/passenger_data.csv /bda_project/data/

# 3. View HDFS contents
hdfs dfs -ls /bda_project/data/
```

---

## 🎓 College Lab Evaluation & Viva-Voce Q&A

This project is tailored for a **two-student team** in a **15-Mark Laboratory Evaluation**:

| Student | Assigned Domain | Responsibilities |
|---|---|---|
| **Student 1** | Big Data Pipeline & PySpark Engine | `analysis.py`, schema validation, Spark transformations (`groupBy`, `agg`, `withColumn`, `pivot`), and HDFS integration. |
| **Student 2** | Dashboard, Visual Analytics & UI | `app.py`, KPI metrics cards, Plotly/Matplotlib charts, sidebar filters, file uploader, and deployment. |

### Common Viva Questions & Answers:

1. **Q: Why use PySpark instead of pure Pandas for transit analytics?**  
   *A:* Pandas processes data in single-machine memory. PySpark utilizes Apache Spark's distributed computing engine, dividing data across worker partitions (RDDs/DataFrames) and executing in-memory transformations in parallel. This enables scaling to millions of daily transit GPS/tap-in records.

2. **Q: What is the difference between Spark Transformations and Actions?**  
   *A:* Transformations (`withColumn`, `filter`, `groupBy`) are evaluated lazily and build a Directed Acyclic Graph (DAG) execution plan. Actions (`count`, `collect`, `show`, `toPandas`) trigger physical execution across the cluster.

3. **Q: How does the system handle missing values or incorrect schema?**  
   *A:* `validate_dataset_schema()` checks for required column headers (`Date`, `Route`, `TimeSlot`, `Passengers`, `BusesOperating`), identifies non-numeric inputs, flags null counts, and cleans data gracefully in `clean_dataset()`.

4. **Q: How does the Route x Time Slot matrix work in PySpark?**  
   *A:* It uses Spark's `groupBy("Route").pivot("TimeSlot").sum("Passengers")` to perform a distributed two-dimensional transposition.

---

## 🐙 Publishing to GitHub (Git Step-by-Step Guide)

Follow these exact commands to publish this completed project to your public GitHub profile:

### 1. Initialize Local Git Repository
```powershell
cd "e:\BDA Project"
git init
```

### 2. Stage All Project Files
```powershell
git add .
```

### 3. Verify Staged Files
```powershell
git status
```
*(Verify that `passenger_data.csv`, `app.py`, `analysis.py`, `requirements.txt`, and `README.md` are staged, and `.venv`/cache files are excluded by `.gitignore`)*

### 4. Commit Changes
```powershell
git commit -m "Initial commit: Public Transport Passenger Demand Analytics using PySpark & Streamlit"
```

### 5. Create a New Public Repository on GitHub
1. Log in to [GitHub](https://github.com).
2. Click the **+** icon in the top right corner and select **New repository**.
3. Name the repository: `public-transport-demand-analytics-pyspark`.
4. Set visibility to **Public**.
5. **Do not** check "Add a README file" or ".gitignore" (these are already created locally).
6. Click **Create repository**.

### 6. Link and Push to GitHub
Copy your repository URL from GitHub and run:
```powershell
# Set default branch to main
git branch -M main

# Add your remote GitHub repository (replace USERNAME and REPO_NAME with yours)
git remote add origin https://github.com/USERNAME/public-transport-demand-analytics-pyspark.git

# Push code to GitHub
git push -u origin main
```

---

## 📜 License
This project is open-source under the MIT License for educational and academic purposes.
