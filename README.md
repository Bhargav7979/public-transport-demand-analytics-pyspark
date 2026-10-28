# Public Transport Passenger Demand Analytics

## About the Project

This is a Big Data Analytics laboratory project that analyzes passenger demand across different public transport routes and time slots. The aim is to understand how passenger counts vary throughout the day and identify routes and time periods with higher demand.

The project uses Python and PySpark to process passenger records and calculate useful statistics. A Streamlit dashboard is used to display the results through charts, tables, and summary metrics.

The project is designed as a starting point for applying Big Data technologies to public transport data. Hadoop HDFS and MapReduce can be incorporated for distributed storage and batch processing, while Apache Cassandra can be used to store the processed results.

## Objectives

- Analyze passenger counts across different routes.
- Calculate total and average passenger counts for each route.
- Identify time slots with higher passenger demand.
- Study changes in passenger counts over multiple days.
- Display the analysis through a simple dashboard.
- Explore the use of Hadoop and other Big Data technologies in transport analytics.

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | Main programming language |
| Apache Spark and PySpark | Data processing and analysis |
| Streamlit | Dashboard interface |
| Pandas and NumPy | Supporting data manipulation |
| Plotly and Matplotlib | Data visualization |
| CSV | Dataset storage format |
| Hadoop HDFS | Distributed storage, if configured |
| Hadoop MapReduce | Batch processing, if implemented |
| Apache Cassandra | Storage of processed results, if integrated |

## Dataset

The project uses `passenger_data.csv`, which contains passenger records for different routes and time slots.

The dataset contains 1,080 records covering 30 days and includes the following columns:

| Column | Description |
|---|---|
| `Date` | Date of the observation |
| `Route` | Name or identifier of the route |
| `TimeSlot` | Time interval for the passenger count |
| `Passengers` | Recorded passenger count |
| `BusesOperating` | Number of buses operating during that interval |

The included dataset is synthetic and intended for academic demonstration. It should not be treated as official public transport data.

## Project Structure

```text
public-transport-demand-analytics-pyspark/
│
├── app.py
├── analysis.py
├── generate_data.py
├── requirements.txt
├── README.md
├── .gitignore
│
└── data/
    └── passenger_data.csv
```

- `app.py` contains the Streamlit dashboard.
- `analysis.py` contains the data-processing and analysis logic.
- `generate_data.py` generates the sample dataset.
- `requirements.txt` lists the Python dependencies.
- `data/passenger_data.csv` contains the passenger records.

## Getting Started

### Requirements

Install the following before running the project:

- Python 3.10 or 3.11
- Java compatible with the selected Apache Spark version
- Git (optional, for cloning the repository)

### 1. Clone the repository

```bash
git clone https://github.com/Bhargav7979/public-transport-demand-analytics-pyspark.git
cd public-transport-demand-analytics-pyspark
```

If you have not published the repository yet, create it on GitHub and update the URL above.

### 2. Create a virtual environment

On Windows:

```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

Make sure Java is installed and configured if you want to run the PySpark analysis.

### 4. Run the analysis

```bash
python analysis.py
```

This runs the standalone analysis script. The exact output depends on the implementation and the dataset being used.

### 5. Launch the dashboard

```bash
streamlit run app.py
```

Open the local address displayed in the terminal, usually `http://localhost:8501`.

## Project Features

The project is intended to support the following analyses:

- **Route analysis:** Compare passenger counts across routes.
- **Time-slot analysis:** Identify intervals with higher recorded passenger demand.
- **Daily trends:** Observe how passenger counts change over time.
- **Data exploration:** Inspect passenger records and use a compatible CSV dataset.
- **Visualizations:** Present summaries using charts and tables.

The availability of each feature should be verified against the current application code.

## Big Data Concepts

The project focuses on Spark-based analytics and can be extended to demonstrate additional Big Data technologies.

**Hadoop HDFS:** Can be used to store passenger datasets in a distributed file system.

**Hadoop MapReduce:** Can be used to calculate route-wise passenger totals through map and reduce operations.

**Apache Spark:** Processes the data and performs aggregations, such as route comparisons and daily trend analysis.

**PySpark:** Provides the Python interface used to write Spark applications.

**Apache Cassandra:** Can store processed summaries for retrieval by the dashboard.

HDFS, MapReduce, and Cassandra should only be considered completed project components after they have been configured and tested. Spark and PySpark are not separate processing engines; PySpark is the Python API for Spark.

## Limitations

- The included dataset is synthetic and does not represent verified passenger statistics.
- The analysis depends on the quality and coverage of the input data.
- Running Spark locally does not demonstrate execution on a distributed cluster.
- Passenger counts alone cannot establish overcrowding without reliable capacity or occupancy information.
- Additional configuration and testing are required for HDFS, MapReduce, and Cassandra integration.

## Future Improvements

Possible extensions include using a verified public transport dataset, integrating HDFS for distributed storage, implementing a Hadoop MapReduce job, storing processed summaries in Cassandra, and exploring passenger demand forecasting when sufficient historical data is available.

## Academic Use

This project was developed as part of the Big Data Analytics laboratory evaluation at Ramdeobaba University. It is intended for learning and demonstrating data processing, analytics, and visualization concepts.

## References

- Apache Spark: https://spark.apache.org/docs/latest/
- PySpark API: https://spark.apache.org/docs/latest/api/python/
- Apache Hadoop: https://hadoop.apache.org/docs/stable/
- Apache Cassandra: https://cassandra.apache.org/doc/latest/
- Streamlit: https://docs.streamlit.io/
