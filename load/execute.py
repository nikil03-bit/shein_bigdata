import sys
import time
import os
import psycopg2
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time  


def create_spark_session(logger):
    """Initialize Spark session with PostgreSQL JDBC driver"""
    logger.debug("Initializing Spark Session with default parameters")
    return SparkSession.builder \
        .appName("SheinDataLoad") \
        .getOrCreate()


def create_postgres_tables(logger, pg_un, pg_pw):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=pg_un,
            password=pg_pw,
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        logger.debug("Successfully connected to postgres database")

        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS master_table (
                product_id VARCHAR PRIMARY KEY,
                title TEXT,
                category TEXT,
                price NUMERIC,
                discount VARCHAR,
                selling_proposition TEXT,
                price_num NUMERIC,
                discount_num INT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS category_stats (
                category TEXT PRIMARY KEY,
                product_count INT,
                avg_price NUMERIC,
                avg_discount NUMERIC
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS products_metadata (
                product_id VARCHAR PRIMARY KEY,
                title TEXT,
                category TEXT,
                price NUMERIC,
                discount VARCHAR
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS discount_table (
                product_id VARCHAR,
                category TEXT,
                discount VARCHAR
            );
            """
        ]

        for query in create_table_queries:
            cursor.execute(query)
        conn.commit()
        logger.info("PostgreSQL tables created successfully")

    except Exception as e:
        logger.warning(f"Error creating tables: {e}")
    finally:
        logger.debug("Closing connection and cursor to postgres db")
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def load_to_postgres(logger, spark, input_dir, pg_un, pg_pw):
    """Load Parquet files to PostgreSQL."""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": pg_un,
        "password": pg_pw,
        "driver": "org.postgresql.Driver"
    }

    tables = [
        ("stage2/master_table", "master_table"),
        ("stage2/category_stats", "category_stats"),
        ("stage3/products_metadata", "products_metadata"),
        ("stage3/discount_table", "discount_table")
    ]

    for parquet_path, table_name in tables:
        try:
            df = spark.read.parquet(os.path.join(input_dir, parquet_path))
            mode = "append" if "master" in parquet_path else "overwrite"
            df.write \
                .mode(mode) \
                .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            logger.info(f"Loaded {table_name} to PostgreSQL")
        except Exception as e:
            logger.warning(f"Error loading {table_name}: {e}")


if __name__ == "__main__":

    logger = setup_logging("load.log")

    if len(sys.argv) != 4:
        logger.error("Usage: python load/execute.py <input_dir> <pg_username> <pg_password>")
        sys.exit(1)

    input_dir = sys.argv[1]
    pg_un = sys.argv[2]
    pg_pw = sys.argv[3]

    if not os.path.exists(input_dir):
        logger.error(f"Error: Input directory {input_dir} does not exist")
        sys.exit(1)

    logger.info("Load Stage started")
    start = time.time()

    spark = create_spark_session(logger)
    create_postgres_tables(logger, pg_un, pg_pw)
    load_to_postgres(logger, spark, input_dir, pg_un, pg_pw)

    end = time.time()
    logger.info("Load Stage completed")
    logger.info(f"Total time taken {format_time(end - start)}")
