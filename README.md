# HDFS anomaly app — Docker / Kafka

This packages the uploaded, working whole-job pipeline: one conversion consumer with local multiprocessing, followed by one prediction consumer. It does not enable distributed chunk parsing or run Celery/Redis.

## Included services

| Service | Purpose |
|---|---|
| web | Django REST API via Gunicorn, localhost port 8000 |
| conversion | Kafka conversion consumer; local parser subprocesses; in-memory block merging |
| prediction | Kafka prediction consumer |
| postgres | Shared PostgreSQL 16 database, persistent volume |
| kafka | Local single-broker Kafka 4.0, persistent volume |
| kafka-init | Creates log-jobs (4 partitions) and prediction-jobs (1 partition) |
| migrate | Applies committed Django migrations before application startup |
| logs | Serves existing host log files internally at http://logs:9000 |

The app uses kafka:9092 and postgres:5432 inside Docker. Kafka/PostgreSQL/log-server ports are not published to the Mac. Existing host services on ports 9092 and 9000 can therefore coexist, although stop the old API if it occupies port 8000.

## 1. Prepare configuration

Extract this archive into a new folder and run all commands from the folder containing compose.yaml.

```bash
cp docker.env.example docker.env
```

Edit docker.env. Set a random DJANGO_SECRET_KEY and POSTGRES_PASSWORD, and verify these absolute host directories:

- HDFS_MODEL_DIR: contains anomaly_model.joblib
- HDFS_TEMPLATE_DIR: contains HDFS.log_templates.csv
- HDFS_LOG_DIR: contains sample_hdfs.log and HDFS.log

The supplied example uses your existing Mac paths. Directory values may contain spaces; they are parsed by Compose. Files are mounted read-only, not copied into the image. Existing directories must exist; Compose will fail instead of silently creating empty bind directories.

Use docker.env, not .env, because your Python virtual environment may already be named .env. Compose's --env-file flag loads path interpolation; env_file also passes app settings into containers. Do not commit docker.env.

Parser defaults preserve the uploaded settings: 10 workers, 200,000-line chunks, 100,000-object database batches. Set LOG_CHUNK_SIZE=100000 if you want the earlier 112-chunk configuration. The conversion/prediction algorithms are unchanged.

## 2. Build and start

Docker Desktop must be running. Stop the old host Django API and any consumers you no longer want running. Let active jobs finish first.

```bash
docker compose --env-file docker.env config --quiet
docker compose --env-file docker.env up --build -d
docker compose --env-file docker.env ps -a
```

kafka-init and migrate should exit with code 0. They are one-time setup services, not crashed workers. The other services should run. Initial builds and Kafka startup may take several minutes.

```bash
docker compose --env-file docker.env logs --tail=80 web conversion prediction
```

This starts a NEW PostgreSQL database. It does not import your SQLite jobs, users, or tokens. Your original SQLite file is not included or modified. The existing model and templates remain mounted from your Mac. Do not reuse a host-only log URL in container jobs.

## 3. Create an admin and token

```bash
docker compose --env-file docker.env exec web python manage.py createsuperuser --username adam
docker compose --env-file docker.env exec web python manage.py drf_create_token adam
```

Use the printed token from this new database. Your old token is unavailable unless its user/token records are explicitly migrated. Paste the new token when prompted:

```bash
read -r API_TOKEN
export API_TOKEN
```

## 4. Verify resources and health

```bash
curl http://127.0.0.1:8000/api/
docker compose --env-file docker.env exec web python manage.py check
docker compose --env-file docker.env exec web python manage.py shell -c 'from log_analyzer.services import load_resources; r = load_resources(); print("Events:", len(r["event_ids"]), "Templates:", len(r["templates"]))'
```

Expected resources: 29 events and 29 templates for your supplied HDFS model. If loading reports a scikit-learn incompatibility, preserve the training dependency versions; do not blindly upgrade or downgrade the model environment. The uploaded dependency pins were retained, with PostgreSQL and Gunicorn added.

GET /api/ now returns a health JSON response. The uploaded index view deleted jobs, features, and predictions; that deletion was removed.

## 5. Submit a sample job

```bash
curl -sS -X POST 'http://127.0.0.1:8000/api/jobs/' \
  -H "Authorization: Token $API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"source_url":"http://logs:9000/sample_hdfs.log"}'
```

Follow processing:

```bash
docker compose --env-file docker.env logs -f conversion prediction
```

Ctrl+C exits log-following only; it does not stop the containers. Copy the returned job UUID:

```bash
JOB_ID='replace-with-returned-job-id'
curl -sS "http://127.0.0.1:8000/api/jobs/$JOB_ID/" \
  -H "Authorization: Token $API_TOKEN"
curl -sS "http://127.0.0.1:8000/api/jobs/$JOB_ID/predictions/" \
  -H "Authorization: Token $API_TOKEN"
```

For your unchanged sample and model, expect 415 blocks, 118 Normal and 297 Anomaly. Prediction remains a separate asynchronous stage; wait until status is completed.

## 6. Submit the full file

```bash
curl -sS -X POST 'http://127.0.0.1:8000/api/jobs/' \
  -H "Authorization: Token $API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"source_url":"http://logs:9000/HDFS.log"}'
```

Expected unchanged-data totals: 11,175,629 matched lines, 575,061 blocks, 557,539 Normal and 17,522 Anomaly. Timings are machine dependent; measure again in Docker, especially after switching to PostgreSQL. Assign enough CPU/RAM to Docker Desktop for the chosen parser count.

## 7. Inspect Kafka

```bash
docker compose --env-file docker.env exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group hdfs-job-processors --members --verbose
docker compose --env-file docker.env exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group hdfs-prediction-processors
```

The default one conversion consumer owns all four log-jobs partitions. Multiple consumers distribute separate jobs, not one file. Do not scale this during an active job: existing interrupted-job/rebalance recovery still requires manual inspection. Four conversion replicas with 10 parser workers each could launch 40 local subprocesses. PostgreSQL removes SQLite file-lock contention, but does not implement job leases or automatic recovery.

## 8. Stop / rebuild

Allow current jobs to complete before stopping, because in-memory aggregation is not checkpointed.

```bash
docker compose --env-file docker.env stop
docker compose --env-file docker.env up --build -d
```

Database and Kafka data persist in named volumes. Do not use down -v unless you intentionally want to erase both data stores.

## Changes from the upload

- Added Dockerfile, Compose, docker.env.example, .dockerignore, and this guide.
- Added the missing initial log_analyzer migration; the upload contained no migrations.
- Added environment-based database, Kafka, model/template, and URL configuration.
- Docker uses PostgreSQL; running without POSTGRES_HOST retains the SQLite fallback.
- Removed automatic Celery import and the unused Celery import from views; legacy task files/dependencies remain inactive.
- Replaced destructive index GET with a health response.
- Kept conversion, parser, prediction, and Kafka handoff algorithms unchanged.
- Excluded the uploaded database, bytecode, secrets, logs, and trained model from the delivered archive.

## Validation and limitations

Validated here: dependency installation, Django system checks, fresh migrations, no pending model migrations, PostgreSQL backend/driver configuration, Python syntax, YAML structure/dependency references, and a small synthetic test exercising two spawned parser processes, cross-chunk merging, feature/prediction writes, API job dispatch (Kafka publisher mocked), retrieval, and non-destructive health behavior.

Docker is unavailable in the validation environment, so image build, live Kafka/PostgreSQL integration, Apple Silicon behavior, and full-file/model execution have not been tested here. Run the sample checks above before the full file. This is a local development Compose stack: single Kafka broker, local credentials/plaintext private network, and manual interrupted-job recovery. It is not a Kubernetes production deployment.
