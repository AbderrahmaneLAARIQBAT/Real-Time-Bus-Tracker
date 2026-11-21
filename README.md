# 🚌 Smart Bus ETA Prediction System

A **real-time Big Data project** that simulates public bus movements, processes live GPS data through Apache Kafka and PySpark, calculates the **Estimated Time of Arrival (ETA)** for each station, and stores the results in **HDFS and Hive** for batch analytics.

> ✅ Built entirely using open-source tools  
> ✅ Runs locally (no cloud required)  
> ✅ Suitable for educational and portfolio use

---

## 🎯 Project Goals

- Simulate real-time public bus GPS data.
- Process and clean the stream using **Spark Structured Streaming**.
- Compute ETA per station based on speed and distance.
- Store results as **Parquet files in HDFS**.
- Query ETA results via **Hive** (external table).
- Build a scalable pipeline replicating production-level architecture.

---

## 🧱 Technologies Used

| Technology        | Role                                     |
|-------------------|------------------------------------------|
| Apache Kafka      | Real-time message broker (GPS data)      |
| PySpark           | Streaming and batch processing engine    |
| HDFS              | Distributed file system for storage      |
| Hive              | SQL analytics on Parquet (external table)|
| confluent_kafka   | Python client for Kafka                  |

---

## 🖼 Architecture

```
           +------------------+        +--------------------------+
           | Python Producer  |----->  |     Kafka Topic          |
           |  (GPS Stream)    |        |    'bus_gps_topic'       |
           +------------------+        +--------------------------+
                                                  |
                                                  v
                                  +-----------------------------+
                                  | Spark Structured Streaming  |
                                  |  - Cleans + joins metadata  |
                                  |  - Computes ETA             |
                                  +-----------------------------+
                                                  |
                                                  v
                           +------------------> HDFS (/smartbus/eta_results)
                           |                   (Parquet files)
                           |
                           +-------------------> Hive (external table)

```

---

## 🗂 Project Structure

```
SmartBus-ETA/
├── kafka/
│   └── bus_gps_producer.py         # Simulates GPS messages
├── spark_processing/
│   ├── analysis.py         
│   └── stream_eta_calculator.py    # Calculates ETA
├── hive/
│   └── hive.sql                    # Hive table definition
├── hdfs/                            # Local HDFS path for Parquet files
├── spark_streaming 
│    └──    stream_processor.py      # Reads from Kafka and stores to HDFS                  
├── requirements.txt
└── README.md
```

---

## ⚙️ How to Run the Project

### 1️⃣ Start HDFS

```bash
start-dfs.cmd    # or start-dfs.sh if on Linux
```

### 2️⃣ Start Zookeeper

```bash
zookeeper-server-start.bat config/zookeeper.properties
```

### 3️⃣ Start Kafka

```bash
kafka-server-start.bat config/server.properties
```

### 4️⃣ Create Kafka Topic

```bash
kafka-topics.bat --create --topic bus_gps_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

### 5️⃣ Run the Kafka Producer (Python)

```bash
cd kafka
python bus_gps_producer.py
```

> 📡 This will send fake GPS messages to Kafka in real-time.

---

### 6️⃣ Run Spark Stream Processor

```bash
cd spark_processing
spark-submit stream_processor.py
```

> ⚙️ Reads from Kafka → processes data → writes clean GPS data to HDFS

---

### 7️⃣ Run ETA Calculator (PySpark)

```bash
spark-submit stream_eta_calculator.py
```

> 🕒 Computes ETA based on current speed and station distance, stores in `/smartbus/eta_results`

---

### 8️⃣ Load Results into Hive

Start Hive CLI:

```bash
hive
```

Then execute:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS eta_predictions (
    bus_id STRING,
    line_id STRING,
    station_name STRING,
    distance_km DOUBLE,
    speed DOUBLE,
    eta_minutes DOUBLE
)
STORED AS PARQUET
LOCATION '/smartbus/eta_results';
```

Now you can query your results:

```sql
SELECT * FROM eta_predictions LIMIT 10;
```

---

## 📌 Sample Output (in Hive)

| bus_id | line_id | station_name | distance_km | speed | eta_minutes |
|--------|---------|--------------|-------------|-------|-------------|
| BUS_5  | L2      | Station B    |     4.1     | 25.3  |    9.73     |
| BUS_2  | L1      | Station A    |     2.5     | 30.0  |    5.00     |

---

## 📦 Requirements

Create a `requirements.txt`:

```txt
confluent_kafka
pyspark
```

Install using:

```bash
pip install -r requirements.txt
```

You also need:
- Java 8 or 11
- Hadoop (HDFS) configured on `localhost:9000`
- Hive installed with Derby or MySQL Metastore

---

## 💡 Future Improvements

- Integrate with Metabase or Grafana for real-time dashboards.
- Predict ETA using historical ML model.
- Add REST API with Flask to serve live ETAs.

---

## 👨‍💻 Author

**Amine Hassnioui**  
🚀 Building real-world Big Data projects  
📍 Morocco  
🧠 Focus: Spark, HDFS, Kafka, Hive, NiFi, Airflow

---

## 🏷 License

This project is open-source under the MIT License.