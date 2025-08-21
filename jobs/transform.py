from pyspark.sql import SparkSession
import logging, os

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

def main():

    try:
        spark = SparkSession.builder \
            .appName("Transform_Stage") \
            .master("spark://spark-master:7077") \
            .getOrCreate()

        input_path = "/opt/airflow/data/input/"
        processed_path = "/opt/airflow/data/processed/"
        os.makedirs(processed_path, exist_ok=True)

        dom = spark.read.csv(f"{input_path}domicile_info.csv", header=True, inferSchema=True)
        loc = spark.read.csv(f"{input_path}location_info.csv", header=True, inferSchema=True)
        pay = spark.read.csv(f"{input_path}pay_level_info.csv", header=True, inferSchema=True)
        dept = spark.read.csv(f"{input_path}dept_info.csv", header=True, inferSchema=True)
        post = spark.read.csv(f"{input_path}post_info.csv", header=True, inferSchema=True)
        qual = spark.read.csv(f"{input_path}qual_info.csv", header=True, inferSchema=True)
        source = spark.read.csv(f"{input_path}source_info.csv", header=True, inferSchema=True)
        cadre = spark.read.csv(f"{input_path}cadre_info.csv", header=True, inferSchema=True)
        officers = spark.read.csv(f"{input_path}officers.csv", header=True, inferSchema=True)

        log.info("Joining tables...")
        officers = officers \
            .join(qual, "qual_id") \
            .join(source, "source_id") \
            .join(post, "post_id") \
            .join(dept, "dept_id") \
            .join(dom, "dom_id") \
            .join(cadre, "cadre_id") \
            .join(loc, "loc_id") \
            .join(pay, "level_id") \
            .drop('qual_id', 'source_id', 'post_id', 'dept_id', 'dom_id', 'cadre_id', 'loc_id', 'level_id')


        df = officers.toPandas()
        df.to_csv(f"{processed_path}officers.csv", index=False)
        log.info(f'................Table officers transformed saved as csv..........')

    except Exception as e:
        log.error(e)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
