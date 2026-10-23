
# Real-Time E-Commerce Order Streaming Pipeline

A real-time data engineering project that ingests e-commerce order events using Apache Kafka, processes streaming data with PySpark, and stores data in a MinIO data lake using Bronze, Silver, and Gold layers.

## Architecture

Python Order Producer → Apache Kafka → PySpark Structured Streaming → MinIO Data Lake

- **Bronze:** Raw Kafka events stored as JSON.
- **Silver:** Parsed and validated order records stored as Parquet.
- **Gold:** Aggregated sales metrics by product category, stored as Parquet.
- **Quarantine:** Separate storage for invalid order events.
- **Testing:** Unit tests for order validation.

## Technology Stack

- Python 3.10
- Apache Kafka 3.9.1
- PySpark 3.5.5
- MinIO
- Docker Compose
- JSON and Apache Parquet
- Python unittest

## Project Structure

```text
kafka-orders-streaming/
├── producer/
│   └── order_producer.py
├── pipeline/
│   ├── Bronze.py
│   ├── Silver.py
│   ├── Gold.py
│   └── quarantine.py
├── tests/
│   └── test_order_validation.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Data Flow

1. The Python producer generates sample e-commerce order events.
2. Kafka receives events in the `ecommerce-orders` topic.
3. The Bronze pipeline stores raw events and Kafka metadata in MinIO.
4. The Silver pipeline parses the events and filters invalid records.
5. The Gold pipeline aggregates order counts, units sold, and sales by category.
6. The Quarantine pipeline writes invalid events separately.
7. Unit tests validate order data rules.

## Prerequisites

- Python 3.10
- Docker Desktop
- Java compatible with PySpark 3.5.5
- IntelliJ IDEA or another Python IDE

## Setup

### 1. Start Kafka and MinIO

```bash
docker compose up -d
```

### 2. Create the Kafka topic

```bash
docker exec orders-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic ecommerce-orders \
  --partitions 3 \
  --replication-factor 1
```

If the topic already exists, continue to the next step.

### 3. Install dependencies

Activate your virtual environment and run:

```bash
pip install -r requirements.txt
```

### 4. Start the order producer

Run:

```bash
python producer/order_producer.py
```

### 5. Start the pipelines

Run these scripts from the IDE, using separate run sessions:

1. `pipeline/Bronze.py`
2. `pipeline/Silver.py`
3. `pipeline/Gold.py`
4. `pipeline/quarantine.py`

Ensure Kafka and MinIO are running before starting the pipelines.

## MinIO Console

Open [http://localhost:9001](http://localhost:9001) to inspect stored data.

The configured bucket is `orders`, with these data locations:

- `bronze/orders/`
- `silver/orders/`
- `gold/category_sales/`
- `quarantine/orders/`

Use the local development credentials configured in `docker-compose.yml`. Do not use development credentials in a production deployment.

## Run Unit Tests

```bash
python -m unittest discover -s tests -v
```

The current order validation test suite contains six tests.

## Future Improvements

- Add malformed and invalid event generation.
- Improve quarantine reason tracking and timestamp validation.
- Add duplicate-event detection and handling of late events.
- Improve checkpoint and recovery testing.
- Add automated integration tests.
- Add orchestration and monitoring.
- Configure production-grade secrets and deployment settings.

## Disclaimer

This project is intended for local development and learning. It uses sample order data and local infrastructure.
