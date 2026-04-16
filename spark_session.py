from pyspark.sql import SparkSession

DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"

def get_spark(app_name: str = "projeto-ed-satc") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Delta Lake
        .config("spark.jars.packages", f"{DELTA_PACKAGE},{ICEBERG_PACKAGE}")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension,"
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Iceberg local catalog
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "warehouse/iceberg")
        .getOrCreate()
    )