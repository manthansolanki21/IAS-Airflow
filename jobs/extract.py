from pyspark.sql import SparkSession
import logging, os, sys

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

def main():

    try:

        spark = SparkSession.builder \
            .appName("Extract_Stage") \
            .master("spark://spark-master:7077") \
            .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.3.jar") \
            .getOrCreate()

        jdbc_url = "jdbc:postgresql://host.docker.internal:5432/IAS?currentSchema=IAS"
        props = {"user": "postgres", "password": "<password>", "driver": "org.postgresql.Driver"}

        tables = ["domicile_info", "location_info", "pay_level_info", "dept_info",
                "post_info", "qual_info", "source_info", "cadre_info", "officers"]

        output_path = "/opt/airflow/data/input/"
        os.makedirs(output_path, exist_ok=True)

        for tbl in tables:
            log.info(f"Extracting {tbl}")
            
            df = spark.read.jdbc(url=jdbc_url, table=f'public.{tbl}', properties=props)
            df = df.toPandas()
            df.to_csv(f"{output_path}{tbl}.csv", index=False)
            
            log.info(f'............Table {tbl} saved as csv............')

    except Exception as e:
        log.error(e)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
