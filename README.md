# OLake Walkthrough

Hands-on demo for building a lakehouse with **OLake**: sync MySQL CDC data into **Apache Iceberg** on **MinIO**, then query the same tables with **Trino**.

---

## Part 1: Sync data to Iceberg and Query it using Spark and Trino

### OLake Playground

The OLake playground lives in the official OLake GitHub repo. Use it to spin up MySQL, MinIO, Iceberg REST catalog, Spark, and the OLake UI — then create a pipeline and sync data to Iceberg.

| Resource | Link |
|----------|------|
| **OLake GitHub** | [github.com/datazip-inc/olake](https://github.com/datazip-inc/olake) |
| **Playground examples** | [github.com/datazip-inc/olake/tree/master/examples](https://github.com/datazip-inc/olake/tree/master/examples) |
| **Spark + MinIO + MySQL example** (used in this walkthrough) | [spark-tablurarest-minio-mysql](https://github.com/datazip-inc/olake/tree/master/examples/spark-tablurarest-minio-mysql) |
| **OLake UI stack** | [github.com/datazip-inc/olake-ui](https://github.com/datazip-inc/olake-ui) |

Quick start from the playground:

```bash
# 1) Start OLake UI stack
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose-v1.yml | ENABLE_OPTIMIZATION="true" docker compose --profile fusion -f - up -d

# 2) Clone OLake and start the example stack
git clone https://github.com/datazip-inc/olake.git
cd olake/examples/spark-tablurarest-minio-mysql
docker compose up -d
```

Then follow the [example README](https://github.com/datazip-inc/olake/tree/master/examples/spark-tablurarest-minio-mysql) to configure a job in OLake UI and run your first sync.

Once data is in Iceberg, continue below to query it with Trino.

---

### Step 1: Clone this repo and start Trino

```bash
git clone https://github.com/nayanj98/OLake-walkthrough.git
cd OLake-walkthrough
```

This repo includes `docker-compose-trino.yml` and the `trino/etc/` config. Trino connects to the **existing** MinIO and Iceberg REST catalog on `olake-network`.

Start Trino + SQLPad:

```bash
docker compose -f docker-compose-trino.yml up -d
```

---

### Step 2: Query your synced data

#### Option A: Trino CLI

```bash
# List schemas
docker exec -it olake-trino-coordinator trino \
  --catalog iceberg --execute "SHOW SCHEMAS;"

# List tables
docker exec -it olake-trino-coordinator trino \
  --catalog iceberg --schema job_weather --execute "SHOW TABLES;"

# Query synced data
docker exec -it olake-trino-coordinator trino \
  --catalog iceberg --schema job_weather \
  --execute "SELECT * FROM weather LIMIT 10;"
```

#### Option B: SQLPad UI

Open [http://localhost:3000](http://localhost:3000) — login: `admin` / `password`

Run:

```sql
SELECT * FROM job_weather.weather LIMIT 10;
```

#### Option C: Trino Web UI

Open [http://localhost:8090](http://localhost:8090)

---

## Part 2: TPCH CDC demo (partsupp)

This section loads a larger dataset into MySQL, syncs it to Iceberg via OLake, runs continuous updates to simulate CDC, and watches changes appear in Trino.

### Prerequisites

- Python 3.10+
- MySQL running (from the Spark stack)

### Ports used (full stack)

| Port | Service |
|------|---------|
| 8000 | OLake UI |
| 3306 | MySQL |
| 8181 | Iceberg REST catalog |
| 9000 | MinIO API |
| 9091 | MinIO console |

---

### Step 3: Generate TPCH data in MySQL

```bash
pip install -r requirements-postgres-to-mysql.txt

python3 duckdb_to_mysql.py
```

This uses **DuckDB** to generate a full TPCH dataset at **scale factor 10 (~10 GB total)**, then loads only **`partsupp`** into MySQL (`tpch.partsupp`, ~8M rows).

Verify:

```bash
docker exec -it primary_mysql mysql -u root -ppassword -e "
  USE tpch;
  SELECT COUNT(*) FROM partsupp;
  SELECT * FROM partsupp LIMIT 5;
"
```

---

### Step 4: Configure OLake — sync partsupp to Iceberg

In OLake UI ([http://localhost:8000](http://localhost:8000), `admin` / `password`), create a new **Job** named `tpch_job`.

#### Source (MySQL)

| Field | Value |
|-------|-------|
| Connector | MySQL |
| Host | `host.docker.internal` |
| Port | `3306` |
| Database | `tpch` |
| Username | `root` |
| Password | `password` |

Select **`partsupp`** and enable **Normalisation**.

#### Destination (Apache Iceberg)

| Field | Value |
|-------|-------|
| Connector | Apache Iceberg |
| Catalog Type | REST |
| REST Catalog URI | `http://host.docker.internal:8181` |
| S3 Path | `s3://warehouse/tpch_job/` |
| S3 Endpoint | `http://host.docker.internal:9000` |
| S3 Access Key | `minio` |
| S3 Secret Key | `minio123` |
| AWS Region | `us-east-1` |

Click **Sync now** and wait for completion. Iceberg table: `tpch_job.partsupp`.

Query in Trino:

```bash
docker exec -it olake-trino-coordinator trino \
  --catalog iceberg --schema tpch_job \
  --execute "SELECT COUNT(*) FROM partsupp;"
```

---

### Step 5: Start continuous MySQL updates (CDC)

In a **separate terminal**:

```bash
python3 continuous_update_partsupp.py
```

This adds **+10** to `ps_supplycost` on **500,000 rows every minute**, generating MySQL binlog / CDC events.

Example output:

```
[run 1] Updating first 500,000 rows in tpch.partsupp (+10 to ps_supplycost) ...
[run 1] Done. Rows updated: 500,000 | Time taken: 3.2s
  Sleeping 56.8s until next run ...
```

---

### Step 6: Sync CDC changes in OLake

Set the OLake job frequency to **Every minute**, or click **Sync now** each minute during the demo.

After each sync, query Trino again and watch `ps_supplycost` change:

```sql
SELECT ps_partkey, ps_suppkey, ps_supplycost
FROM tpch_job.partsupp
LIMIT 10;
```

New Iceberg metadata/data files will also appear in MinIO: [http://localhost:9091](http://localhost:9091) (`minio` / `minio123`) under `warehouse/tpch_job/partsupp/`.

---

## Repo contents

| File | Purpose |
|------|---------|
| `docker-compose-trino.yml` | Trino + SQLPad (connects to existing MinIO/Iceberg stack) |
| `trino/etc/` | Trino config (Iceberg REST catalog + MinIO S3) |
| `postgres_to_mysql.py` | Generate TPCH SF=10 data; load `partsupp` into MySQL |
| `continuous_update_partsupp.py` | Update 500k rows/min in MySQL for CDC testing |
| `requirements-postgres-to-mysql.txt` | Python dependencies (`duckdb`, `pymysql`) |

---

## Reset MinIO / Iceberg catalog (fresh start)

```bash
# 1. Empty MinIO warehouse bucket (keeps bucket)
docker run --rm --network olake-network --entrypoint /bin/sh minio/mc -c "
  mc alias set myminio http://minio:9000 minio minio123 &&
  mc rm -r --force myminio/warehouse/
"

# 2. Clear Iceberg REST catalog metadata
docker exec temporal-postgresql psql -U temporal -d postgres -c "
  TRUNCATE iceberg_tables, iceberg_namespace_properties;
"

# 3. Restart catalog
docker restart iceberg-rest
```

---

## Troubleshooting

**Trino can't connect / `ENOTFOUND`**  
Ensure Trino stack is up and base stacks are running:

```bash
docker compose -f docker-compose-trino.yml up -d
curl http://localhost:8090/v1/info
```

**SQLPad connection error**  
Use `host.docker.internal:8090` as the Trino host in SQLPad settings.

**OLake can't connect to MySQL**  
Use `host.docker.internal` (not `localhost`) from OLake workers.

**Lock wait timeout on MySQL updates**  
Use the included `continuous_update_partsupp.py` — it uses fast `LIMIT`-based updates.

**Table already exists in catalog**  
Run the reset steps above.

---

## Resources

- [How to Build an Apache Iceberg Lakehouse and Query It with Multiple Engines (Presentation)](https://gamma.app/docs/How-to-Build-an-Apache-Iceberg-Lakehouse-and-Query-It-with-Multip-sfldt4k0nk9s49d?mode=doc)

---

## Demo flow summary

1. **OLake playground:** Set up stacks and sync MySQL → Iceberg ([OLake GitHub](https://github.com/datazip-inc/olake))
2. **This repo — Part 1:** Clone repo → `docker compose -f docker-compose-trino.yml up -d` → query synced data in Trino
3. **Part 2:** Load TPCH `partsupp` → sync via OLake → run continuous updates → sync every minute → watch changes in Trino
