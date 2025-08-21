
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

log = logging.getLogger(__name__)

def extract_transform_load():

    log.info("Starting ETL process...")

    try:
        
        postgres_hook = PostgresHook(postgres_conn_id='ias_airflow')
        conn = postgres_hook.get_conn()

        cur = conn.cursor()
        print("Successfully connected to PostgreSQL database!")

        # Initialize a Spark session
        spark = SparkSession.builder \
            .master("spark://spark-master:7077") \
            .appName("IAS") \
            .getOrCreate()

        cur.execute('''select * from domicile_info''')
        dom = spark.createDataFrame(data = cur.fetchall(), schema=['dom_id', 'domicile'])

        cur.execute('''select * from location_info''')
        loc = spark.createDataFrame(data = cur.fetchall(), schema=['loc_id', 'location'])

        cur.execute('''select * from pay_level_info''')
        pay = spark.createDataFrame(data = cur.fetchall(), schema=['level_id', 'pay_level'])

        cur.execute('''select * from dept_info''')
        dept = spark.createDataFrame(data = cur.fetchall(), schema=['dept_id', 'dept_type'])

        cur.execute('''select * from post_info''')
        post = spark.createDataFrame(data = cur.fetchall(), schema=['post_id', 'post_type'])

        cur.execute('''select * from qual_info''')
        qual = spark.createDataFrame(data = cur.fetchall(), schema=['qual_id', 'qual_type'])

        cur.execute('''select * from source_info''')
        source = spark.createDataFrame(data = cur.fetchall(), schema=['source_id', 'source'])

        cur.execute('''select * from cadre_info''')
        cadre = spark.createDataFrame(data = cur.fetchall(), schema=['cadre_id', 'cadre'])

        cur.execute('''select * from officers''')

        cols = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("date_of_birth", DateType(), True),
            StructField("date_of_appointment", DateType(), True),
            StructField("job_status", StringType(), True),
            StructField("wef_date", DateType(), True),
            StructField("qual_id", IntegerType(), True),
            StructField("dom_id", IntegerType(), True),
            StructField("source_id", IntegerType(), True),
            StructField("cadre_id", IntegerType(), True),
            StructField("post_id", IntegerType(), True),
            StructField("dept_id", IntegerType(), True),
            StructField("loc_id", IntegerType(), True),
            StructField("level_id", IntegerType(), True),
        ])

        officers = spark.createDataFrame(data = cur.fetchall(), schema=cols)

        officers = officers.join(qual, (officers['qual_id'] == qual['qual_id']), 'inner')
        officers = officers.join(source, (officers['source_id'] == source['source_id']), 'inner')
        officers = officers.join(post, (officers['post_id'] == post['post_id']), 'inner')
        officers = officers.join(dept, (officers['dept_id'] == dept['dept_id']), 'inner')
        officers = officers.join(dom, (officers['dom_id'] == dom['dom_id']), 'inner')
        officers = officers.join(cadre, (officers['cadre_id'] == cadre['cadre_id']), 'inner')
        officers = officers.join(loc, (officers['loc_id'] == loc['loc_id']), 'inner')
        officers = officers.join(pay, (officers['level_id'] == pay['level_id']), 'inner')

        officers = officers.drop('qual_id', 'source_id', 'post_id', 'dept_id', 'dom_id', 'cadre_id', 'loc_id', 'level_id')

        df = officers.toPandas()
        output_path = "/opt/airflow/output/officers.csv"

        df.to_csv(output_path, index=False)
        log.info('file saved')


    except psycopg2.OperationalError as e:
        print(f"Database connection failed: {e}")
        raise # Re-raise the exception so Airflow marks the task as failed
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise # Re-raise other exceptions

    finally:
        cur.close()
        conn.close()
        spark.stop()

