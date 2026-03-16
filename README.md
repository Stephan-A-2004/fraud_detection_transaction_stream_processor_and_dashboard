
---

# Real-Time Transaction Fraud Detection System

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Tests](https://img.shields.io/badge/tests-passing-brightgreen)

A **streaming fraud detection pipeline** built with Python that simulates how financial platforms detect suspicious transaction activity in real time. It operates under the assumption that all transactions are stored in a separate database, while this system stores suspicious transactions in a table, and system stats like the average and current processed txn rate per second.

The system generates synthetic transaction events, streams them through **Redis Streams**, processes them with a Python fraud detection service, stores alerts in **PostgreSQL**, and visualises suspicious activity in a **Streamlit monitoring dashboard**.

This project demonstrates several backend and data engineering patterns commonly used in production systems.

---

# Architecture

```
Transaction Generator
        │
        ▼
Redis Stream (transactions)
        │
        ▼
Stream Processor (fraud rules + risk scoring)
        │
        ▼
PostgreSQL (flags table)
        │
        ▼
Streamlit Dashboard (Images of the dashboard can be found in docs)
```

---

# Event Flow

1. The **transaction generator** produces synthetic payment events.
2. Events are written to the Redis Stream `transactions`.
3. The **stream processor** consumes events and maintains a **per-user sliding window**.
4. Fraud detection rules are evaluated against the current window.
5. If a rule triggers, the processor generates an alert and assigns a **risk score**.
6. Alerts are written to PostgreSQL in the `flags` table.
7. The processor records throughput metrics (Transactions processed per second currently, and Transactions processed per second on average) to the `processor_stats` table every 100 events.
8. The **Streamlit dashboard** queries PostgreSQL to visualise alerts, system health and trends.

---

# Pipeline Observability

The stream processor records throughput metrics to a `processor_stats` table in PostgreSQL every 100 events. This includes total events processed, average TPS since startup, and current TPS over the last 100 events.

The dashboard displays these metrics in a **System Health** section, separate from the fraud monitoring views.

---


# Technologies Used

| Component | Technology |
|-----------|------------|
| Language | Python |
| Stream transport | Redis Streams |
| Database | PostgreSQL |
| Dashboard | Streamlit |
| Containerisation | Docker |
| Testing | Pytest |
| Static typing | MyPy |

---

# Fraud Detection Rules

Fraud detection operates on a **60-second sliding window per user**.

For each incoming transaction, the processor evaluates recent activity within that window.

## 1. Velocity + Amount Rule

Trigger when:

* **3 or more transactions**
* **total value > £5000**
* within the window.

Reason stored:

```
velocity_amount
```

---

## 2. High Transaction Velocity

Trigger when:

* **5 or more transactions**
* within the window.

Reason stored:

```
high_velocity
```

---

## 3. Large Transaction Volume

Trigger when:

* **total transaction amount ≥ £10000**
* within the window.

Reason stored:

```
large_transaction
```

---

## 4. Rapid Repeat Merchant Usage

Trigger when:

* **3 or more transactions**
* to the **same merchant**
* within the window.

Reason stored as:

```
rapid_repeat_merchant:<merchant_name>
```

---

# Alert Records

When a rule triggers, an alert is stored in the PostgreSQL `flags` table.

| Field          | Description                                               |
| -------------- | --------------------------------------------------------- |
| `id`           | Auto-generated row ID                                     |
| `user_id`      | User associated with the alert                            |
| `window_start` | Timestamp of earliest transaction in the detection window |
| `window_end`   | Timestamp of latest transaction in the window             |
| `txn_count`    | Number of transactions in the window                      |
| `total_amount` | Sum of transaction values                                 |
| `reason`       | Fraud rule that triggered the alert                       |
| `risk_score`   | Risk score assigned by the processor                      |
| `txn_ids`      | Transaction IDs included in the window                    |
| `dedupe_key`   | Deterministic key used to prevent duplicate alerts        |
| `created_at`   | Timestamp when the alert was stored                       |

---

# Risk Scoring

After a rule triggers, the processor assigns a **risk score between 0 and 100** based on:

* transaction amount
* number of transactions
* rule triggered

The dashboard derives a **risk band** from this score.

| Score | Risk Band |
| ----- | --------- |
| ≥ 80  | High      |
| 50–79 | Medium    |
| < 50  | Low       |

---

# Alert Deduplication

Sliding window detection may trigger repeatedly as new transactions arrive.

To prevent duplicate alerts being stored, the processor generates a deterministic **`dedupe_key`** derived from:

* user ID
* rule triggered
* window timestamps
* transaction IDs

A unique constraint on this key ensures identical alerts are only written once.

---

# Project Structure

```
services/
    common/
        schemas.py
        config.py

    generator/
        Dockerfile
        app/
            main.py
            synth.py

    processor/
        Dockerfile
        app/
            main.py
            consumer.py
            detector.py
            store.py
            txn_parsing.py
            txn_risk_score_calculation.py

infra/
    postgres/
        init.sql

dashboard/
    app.py
    dashboard_connection_db.py
    dashboard_filtering.py
    dashboard_queries.py
    risk_band_assignment_and_dashboard_styling.py
    Dockerfile

tests/
    test_schema.py
    test_schema_validation.py
    test_store_consumer_main.py
    test_txn_parsing_and_risk_scoring.py
    test_processor_main_flow.py
    test_generator_main.py
    test_generator_and_dashboard.py
    test_detector.py
    test_dashboard_queries.py


docs/
    alerts_by_rule_bar_chart.png
    flags_per_minute_chart.png
    tables_part1.png
    tables_part2.png

docker-compose.yaml
requirements.txt
transaction_info.md
mypy.ini
README.md

```

---

# Running the System

Run the following commands:

```
cd file_path_of_root_folder
```

```
docker compose up --build
```

## Accessing dashboard

Go to your internet browser and copy and paste: http://localhost:8501/

Do note, the dashboard can only be accessed while the docker containers are running.

## Stopping the System

Run the below command: 

```
docker compose down
```

## Resetting the database

Run the below command: 

```
docker compose down -v
```


# Logs

All service logs stream to the terminal when running 

```
docker compose up --build
```

When running in detached mode (`docker compose up -d --build`), logs can be viewed with:

```
docker compose logs -f
```

or for a specific service:

```
docker compose logs -f generator
```

```
docker compose logs -f processor
```

```
docker compose logs -f dashboard
```

```
docker compose logs -f redis
```

```
docker compose logs -f postgres
```

Note: To stop seeing logs, ctrl+c must be pressed

# Dashboard Features

## KPI Metrics

* **Flagged transactions** - number of unique transactions that appear in at least one alert window.
* **Total alerts** - number of fraud rule triggers stored in the `flags` table.
* **Unique flagged users** - number of distinct users associated with alerts.
* **Alerts triggered in the last 5 minutes** - short-term alert activity indicator.

Because multiple rules may trigger for the same sliding window, the number of alerts may be higher than the number of flagged transactions.

---

## Charts

### Alerts Over Time

Shows fraud alerts grouped into time buckets with a rolling average.

### Alerts by Rule

Displays how frequently each fraud rule triggers.

Merchant-specific rules are grouped under:

```
rapid_repeat_merchant
```

---

## Tables

### Top Suspicious Users

Users ranked by number of alerts.

### Most Recent Alerts

Latest alerts stored in PostgreSQL.

### Priority Alert Views

Two ranked alert views:

**Largest total amount alerts**

Alerts with the highest total transaction value.

**Highest risk alerts**

Alerts ranked by risk score.

Rows are colour-coded by risk band.

---

## Filters

The dashboard supports:

* timeframe selection
* user filtering
* automatic refresh

---

## System Health

Displays live processor throughput metrics at the bottom of the dashboard, updated every 100 events.

* Total events processed since startup
* Average throughput (transactions per second) since startup
* Current throughput (transactions per second) over the last 100 events
* Time of last recorded measurement

Note: The processed event counter is maintained in memory by the stream processor and resets if the processor container restarts. Alert metrics are persisted in PostgreSQL and therefore may span multiple processor runs.

--- 

# Example Alert

```
user_id: u5
reason: velocity_amount
txn_count: 5
total_amount: 6821.32
risk_score: 85
window_start: 1772684155
window_end: 1772684190
created_at: 2026-03-08T12:34:56Z
```

---

# Why This Project Matters

This project demonstrates several real-world backend patterns used in financial systems:

* streaming event pipelines
* sliding window fraud detection
* deterministic deduplication
* real-time monitoring dashboards
* containerised infrastructure

These are common building blocks in modern fintech and payment platforms.

---

# Possible Future Extension

* Kafka instead of Redis Streams
* Implement risk score calculation for users, based on their transaction history and store this in a different table.

---