from pyspark.sql import SparkSession
import logging, os

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

def main():
    try:

        spark = SparkSession.builder \
            .appName("Load_Stage") \
            .master("spark://spark-master:7077") \
            .getOrCreate()

        processed_path = "/opt/airflow/data/processed/"
        warehouse_path = "/opt/airflow/data/warehouse/"
        os.makedirs(warehouse_path, exist_ok=True)

        officers = spark.read.csv(f"{processed_path}officers.csv", header=True, inferSchema=True)
        df = officers.toPandas()
        df.to_csv(f"{warehouse_path}officers.csv", index=False)


        log.info(f"..........Final CSV saved at {warehouse_path}.........")

    except Exception as e:
        log.error(e)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
