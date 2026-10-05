# HDFS Log Anomaly Detection — Quick Start

A Python pipeline that reads HDFS logs, groups events by block ID, and predicts whether each block is normal or anomalous.

The application runs using Docker Compose with Django, conversion and prediction workers, Kafka, PostgreSQL, and an internal log-file server. Elasticsearch supports searching indexed predictions.

## 1. Configure

Run all commands from the project folder containing `compose.yaml`. Make sure Docker is running and you have POSTGRES_DB 

```bash
cp docker.env.example docker.env
```

Edit `docker.env` and configure:

| Variable | Value |
|---|---|
| `DJANGO_SECRET_KEY` | A random secret |
| `POSTGRES_PASSWORD` | Your database password |
| `HDFS_MODEL_DIR` | Absolute directory containing `anomaly_model.joblib` |
| `HDFS_TEMPLATE_DIR` | Absolute directory containing `HDFS.log_templates.csv` |
| `HDFS_LOG_DIR` | Absolute directory containing `sample_hdfs.log` and `HDFS.log` |

The directories must exist. Files are mounted read-only. Do not commit `docker.env`.

## 2. Build and Start

```bash
docker compose --env-file docker.env config --quiet
docker compose --env-file docker.env up --build -d
docker compose --env-file docker.env ps -a
```

The `migrate` and `kafka-init` services should finish with exit code `0`. Application services should remain running.

This guide assumes the API views allow access **without token authentication**.


## 3 Serving the HDFS Log File

Open a terminal in the folder containing `HDFS.log` and run:

```bash
python3 -m http.server 9005 --bind 0.0.0.0
```

Keep the terminal running while the pipeline processes the file.

The Docker worker can read the file using:

```text
http://host.docker.internal:9005/HDFS.log
```

A `GET /HDFS.log` response with status **200** confirms that the file was accessed successfully.


## 4. Search Predictions

With Elasticsearch configured and running, index saved predictions:

```bash
docker compose --env-file docker.env exec web python manage.py index_predictions
```



## 5. API Endpoints

Base URL: `http://127.0.0.1:8000/api/`

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/` | delete all the jobs |
| POST | `/api/jobs/` | Creates a processing job |
| GET | `/api/jobs/<job_id>/` | Returns job status and statistics |
| GET | `/api/predictions/search/` | Searches indexed predictions by jobID, BlockID, lable to check the result  |



Search using **`job_id`**, **`block_id`**, and **`label`**, individually or together.

**By job ID:**

```bash
curl -sS "http://127.0.0.1:8000/api/predictions/search/?job_id=$JOB_ID"
```

**By block ID:**

```bash
curl -sS "http://127.0.0.1:8000/api/predictions/search/?block_id=blk_-1608999687919862906"
```

**By label:**

```bash
curl -sS "http://127.0.0.1:8000/api/predictions/search/?label=Anomaly"
```

**Combined filters:**

```bash
curl -sS "http://127.0.0.1:8000/api/predictions/search/?job_id=$JOB_ID&block_id=blk_-1608999687919862906&label=Anomaly"
```

Copy the job ID from the response.

Monitor processing:

```bash
docker compose --env-file docker.env logs -f conversion prediction
```

Press **Ctrl+C** to stop following logs. Containers continue running.

For the unchanged supplied sample and model, expected results are **415 blocks: 118 Normal and 297 Anomaly**.



Wait until the job status is `completed`.

## 6. Process the Full Dataset

```bash
curl -sS -X POST http://127.0.0.1:8000/api/jobs/ -H 'Content-Type: application/json' -d '{"source_url":"http://logs:9000/HDFS.log"}'
```

Expected results for the unchanged dataset and model:

| Metric | Result |
|---|---:|
| Matched lines | 11,175,629 |
| Unique blocks | 575,061 |
| Normal | 557,539 |
| Anomaly | 17,522 |

Processing times depend on the available CPU, memory, and database performance.



Start again:

```bash
docker compose --env-file docker.env up -d
```

Rebuild after code changes:

```bash
docker compose --env-file docker.env up --build -d
```

PostgreSQL and Kafka data persist in named volumes. **Do not run `docker compose down -v` unless you intend to delete those data stores.**

## Architecture and Limitations

A conversion consumer parses each file using local multiprocessing, merges event counts, and saves features. A separate prediction consumer loads those features, applies the trained model, and stores results.

Multiple conversion consumers distribute separate jobs; they do not distribute chunks of one file across containers. This is a development setup with a single Kafka broker and manual recovery for interrupted jobs.