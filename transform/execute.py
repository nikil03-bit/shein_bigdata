import os
import sys
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def format_time(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds"


def setup_logger(log_file_name="transform.log"):
    logger = logging.getLogger("transform_logger")
    if logger.hasHandlers():
        return logger
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s')

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fh = logging.FileHandler(log_file_name)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


def create_spark_session(logger):
    logger.debug("Initializing Spark session for Shein transformation")
    return SparkSession.builder.appName("SheinDataTransform").getOrCreate()


def load_and_clean(spark, input_dir, output_dir, logger):
    try:
        all_files = [f for f in os.listdir(input_dir) if f.endswith(".csv")]
        dfs = []

        for file in all_files:
            category = file.replace("us-shein-", "").split("-")[0]  # extract category from filename
            logger.info(f"Reading {file} for category {category}")

            df = spark.read.csv(os.path.join(input_dir, file), header=True, inferSchema=True)

            # Add category + ID
            df = df.withColumn("category", F.lit(category)) \
                   .withColumn("id", F.monotonically_increasing_id())

            # Standardize columns (keep only the important ones if they exist)
            cols_to_keep = []
            if "goods-title-link" in df.columns:
                cols_to_keep.append(F.col("goods-title-link").alias("title"))
            elif "goods-title-link--jump" in df.columns:
                cols_to_keep.append(F.col("goods-title-link--jump").alias("title"))
            else:
                cols_to_keep.append(F.lit(None).alias("title"))

            cols_to_keep.extend([
                F.col("id"),
                F.col("category"),
                F.col("price") if "price" in df.columns else F.lit(None).alias("price"),
                F.col("discount") if "discount" in df.columns else F.lit(None).alias("discount"),
                F.col("selling_proposition") if "selling_proposition" in df.columns else F.lit(None).alias("selling_proposition")
            ])

            df = df.select(*cols_to_keep)
            dfs.append(df)

        if not dfs:
            logger.error("No CSV files found in input directory")
            sys.exit(1)

        products_df = dfs[0]
        for d in dfs[1:]:
            products_df = products_df.unionByName(d, allowMissingColumns=True)

        # Clean
        products_df = products_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())
        products_df = products_df.withColumn("category", F.lower(F.trim(F.col("category"))))

        # Save
        stage1_path = os.path.join(output_dir, "stage1")
        products_df.write.mode("overwrite").parquet(os.path.join(stage1_path, "products"))

        logger.info("Stage 1: Cleaned products data saved")

    except Exception as e:
        logger.error(f"Error in load_and_clean: {e}")
        sys.exit(1)

    return products_df


def create_master_table(output_dir, products_df, logger):
    try:
        # Compute final price if discount is numeric
        master_df = products_df.withColumn(
            "final_price",
            F.when(F.col("discount").isNotNull() & (F.col("discount") != ""),
                   (F.col("price") * (1 - (F.col("discount") / 100.0))))
            .otherwise(F.col("price"))
        )

        stage2_path = os.path.join(output_dir, "stage2")
        master_df.write.mode("overwrite").parquet(os.path.join(stage2_path, "master_table"))

        logger.info("Stage 2: Master table saved")
    except Exception as e:
        logger.error(f"Error in create_master_table: {e}")
        sys.exit(1)


def create_query_tables(output_dir, products_df, logger):
    try:
        stage3_path = os.path.join(output_dir, "stage3")

        # Product metadata table
        products_metadata = products_df.select("id", "title", "category", "price", "discount", "selling_proposition")
        products_metadata.write.mode("overwrite").parquet(os.path.join(stage3_path, "products_metadata"))

        # Category aggregates
        category_stats = products_df.groupBy("category").agg(
            F.count("*").alias("product_count"),
            F.avg("price").alias("avg_price")
        )
        category_stats.write.mode("overwrite").parquet(os.path.join(stage3_path, "category_stats"))

        logger.info("Stage 3: Query-optimized tables saved")
    except Exception as e:
        logger.error(f"Error in create_query_tables: {e}")
        sys.exit(1)


if __name__ == "__main__":
    logger = setup_logger()
    logger.info(f"Script started with args: {sys.argv}")

    if len(sys.argv) != 3:
        logger.error(f"Invalid number of arguments: {len(sys.argv)} received; expected 3 (script, input_dir, output_dir)")
        logger.error("Usage: python transform.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    if not os.path.exists(input_dir):
        logger.error(f"Input directory {input_dir} does not exist")
        sys.exit(1)

    if not os.path.exists(output_dir):
        logger.info(f"Output directory {output_dir} does not exist, creating it.")
        os.makedirs(output_dir)

    start = time.time()
    spark = create_spark_session(logger)

    products_df = load_and_clean(spark, input_dir, output_dir, logger)
    create_master_table(output_dir, products_df, logger)
    create_query_tables(output_dir, products_df, logger)

    end = time.time()
    logger.info("Transformation pipeline completed")
    logger.info(f"Total time taken {format_time(end - start)}")

    spark.stop()
