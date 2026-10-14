"""Minimal Spark job. Add transformations inside main()."""
from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("spark-pipeline-test")
        .master("local[2]")
        .getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("WARN")
        print(f"Spark {spark.version} started with {spark.sparkContext.master}")
        # Add your data loading and transformations here.
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
