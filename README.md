# PyIceberg + MinIO + Nessie: A Data Lakehouse for IoT

A **proof of concept** demonstrating how to build a **data lakehouse** for IoT use cases using **PyIceberg, Nessie, MinIO, and Dremio**. This project enables **near real-time updates** without expensive streaming solutions, and it’s portable, locally testable, and deployable to any cloud platform.

---

## 🚀 **Why This Project?**
Data lakehouses combine the scalability of data lakes with the transactional capabilities of data warehouses. This project showcases:
- **ACID transactions** for IoT data (e.g., sensor updates).
- **Event-driven ingestion** using Lambda-style functions.
- **Portability**: Works locally and can be deployed to **AWS, GCP, Azure, or any cloud**.
- **Cost efficiency**: No need for complex streaming frameworks.

---

## 🛠 **Architecture**
| Component      | Role                                                                 |
|----------------|----------------------------------------------------------------------|
| **PyIceberg**  | Manages Iceberg tables for transactional updates.                   |
| **Nessie**     | Git-like catalog for versioning and managing table metadata.        |
| **MinIO**      | S3-compatible storage for the data lake.                             |
| **Dremio**     | SQL analytics engine for querying data.                              |
| **Lambda**     | Simulates event-driven ingestion (via local HTTP endpoint).          |

---

## 📦 **Prerequisites**
- Docker and Docker Compose
- Python 3.8+
- `curl` and `jq` (for testing)

---

## 🚀 **Quick Start**

### 1. Clone the Repository
```bash
git clone https://github.com/mwa28/pyiceberg-minio-nessie.git
cd pyiceberg-minio-nessie
```

### 2. Start the Stack
```bash
docker-compose up
```
This will spin up:
- MinIO (S3 storage)
- Nessie (catalog)
- Dremio (analytics)
- Local Lambda endpoint

### 3. Upload Sample Data
Upload the provided `sensor-data.jsonl` and `update-data.jsonl` to the `iot-bucket` in MinIO:
```bash
# Example using MinIO client (mc)
mc cp sensor-data.jsonl myminio/iot-bucket/
mc cp update-data.jsonl myminio/iot-bucket/
```

### 4. Simulate IoT Events
Post events to the local Lambda endpoint to simulate real-time ingestion:
```bash
# Post a new sensor event
curl -XPOST "http://localhost:9090/2015-03-31/functions/function/invocations" -d @test/event.json | jq

# Post an update to existing data
curl -XPOST "http://localhost:9090/2015-03-31/functions/function/invocations" -d @test/update.json | jq
```

### 5. Query Data with Dremio
- Access Dremio at `http://localhost:9047`.
- Connect to the Nessie catalog and query the IoT data using SQL.

---

## 📂 **Project Structure**
```
.
├── docker-compose.yml    # Docker setup for MinIO, Nessie, Dremio, and Lambda
├── test/                 # Sample event and update JSON files
│   ├── event.json
│   └── update.json
├── sensor-data.jsonl     # Sample IoT sensor data
├── update-data.jsonl     # Sample updates for IoT data
└── README.md             # This file
```

---

## 🔧 **Customization**
- **Add more data**: Extend `sensor-data.jsonl` or `update-data.jsonl`.
- **Deploy to cloud**: Replace MinIO with AWS S3, GCP Storage, or Azure Blob.
- **Scale**: Use Kubernetes for production deployments.

---

## 📚 **Learn More**
- [Apache Iceberg](https://iceberg.apache.org/)
- [Nessie](https://projectnessie.org/)
- [MinIO](https://min.io/)
- [Dremio](https://www.dremio.com/)

---

## 🤝 **Contribute**
Contributions are welcome! Open an issue or submit a PR.

---

## 📄 **License**
MIT
