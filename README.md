# Lakehouse Walkthrough

Hands-on demo for building a lakehouse with **OLake**: sync MySQL CDC data into **Apache Iceberg** on **MinIO**, then query the same tables with **Trino** and **Spark**.

---

## Part 1: Sync data to Iceberg and Query it using Spark and Trino

### Step 1: Clone this repo

```bash
git clone https://github.com/nayanj98/OLake-walkthrough.git
cd OLake-walkthrough
```

This repo includes `docker-compose-trino.yml` and the `trino/etc/` config used in the steps below.

---

### Step 2: Set up the OLake Playground and sync with Spark

The [OLake Playground](https://github.com/datazip-inc/olake/tree/master/examples/spark-tablurarest-minio-mysql) lives in the official OLake GitHub repo. Use it to spin up MySQL, MinIO, Iceberg REST catalog, Spark, and the OLake UI — then create a pipeline and sync data to Iceberg.

Follow the playground docs **until you have synced your data and queried it with Spark**. Once that is done, come back here to query the same Iceberg tables with Trino.

---

### Step 3: Start Trino

Trino connects to the **existing** MinIO and Iceberg REST catalog on `olake-network` — it does not start MinIO or the catalog itself.

```bash
docker compose -f docker-compose-trino.yml up -d
```

Verify Trino is healthy:

```bash
curl http://localhost:8090/v1/info
```

You should see `"state":"ACTIVE"`.

---

### Step 4: Query your synced data with Trino

Replace `job_weather` with the **namespace matching your OLake job name** (e.g. if your job is named `job`, database is `weather`, schema is `job_weather`).

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

If needed, edit the **OLake Demo** connection:
- **Host:** `host.docker.internal`
- **Port:** `8090`
- **Catalog:** `iceberg`
- **Schema:** `job_weather`

Run:

```sql
SELECT * FROM job_weather.weather LIMIT 10;
```

#### Option C: Trino Web UI

Open [http://localhost:8090](http://localhost:8090)

---

## Part 2: Full load + CDC on TPCH data

This section loads a larger TPCH dataset into MySQL, syncs it to Iceberg via OLake, then runs continuous updates to simulate CDC while syncing changes incrementally.

### Prerequisites

- Python 3.10+
- MySQL running (from the Spark stack)

---

### Step 1: Generate TPCH data in MySQL

Install dependencies:

```bash
pip install -r requirements-postgres-to-mysql.txt
```

Generate TPCH data and load it into MySQL:

```bash
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

### Step 2: Configure OLake — sync partsupp to Iceberg

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

---

### Step 3: Start continuous MySQL updates (CDC)

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

### Step 4: Sync CDC changes in OLake

Set the OLake job frequency to **Every minute**, or click **Sync now** each minute during the demo.

OLake will pick up the MySQL CDC changes and merge them into the Iceberg table on MinIO.

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

## Clean up local DuckDB files

After `duckdb_to_mysql.py` completes and the data is loaded into MySQL, remove the local DuckDB working directory to free disk space (~5 GB):

```bash
rm -rf tpch_workdir
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

---

## Resources

- [How to Build an Apache Iceberg Lakehouse and Query It with Multiple Engines (Presentation)](https://gamma.app/docs/How-to-Build-an-Apache-Iceberg-Lakehouse-and-Query-It-with-Multip-sfldt4k0nk9s49d?mode=doc)

---

## Demo flow summary

1. **Clone this repo** → follow [OLake Playground](https://github.com/datazip-inc/olake/tree/master/examples/spark-tablurarest-minio-mysql) until data is synced and queried in Spark
2. **Part 1 (continued):** `docker compose -f docker-compose-trino.yml up -d` → query the same data in Trino
3. **Part 2:** Load TPCH `partsupp` → sync via OLake → run continuous updates → sync every minute
