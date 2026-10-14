# Spark job starter

A minimal local Docker framework using Python 3.11, Java 17, and PySpark 3.5.3.

## Run

Install and start Docker Desktop, then run from this project folder:

```sh
docker compose run --rm --build spark-job
```

The job creates a Spark session with `local[2]`, prints the Spark version and execution mode, then stops. It has no data sources or transformations yet.

## Add your code

Edit `jobs/main.py` inside the `try` block. The jobs directory is mounted into the container, so Python changes do not require rebuilding the image. Rebuild when changing dependencies or the Dockerfile.

## Files

- `jobs/main.py`: Spark session lifecycle and a placeholder for your job.
- `Dockerfile`: Python and Java runtime.
- `requirements.txt`: PySpark dependency.
- `compose.yaml`: one job service, submitted using `spark-submit`.

Each execution exits when the job completes. A job failure exits unsuccessfully and Spark is stopped in the `finally` block.
