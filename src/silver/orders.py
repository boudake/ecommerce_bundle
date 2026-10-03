from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date

catalog = spark.conf.get("ecommerce.catalog")
schema = spark.conf.get("ecommerce.silver.schema")
bronze_schema = spark.conf.get("ecommerce.bronze.schema")
@dp.table(name=f"{catalog}.{schema}.orders")
def silver_orders():

    return (
        spark.read.table(f"{catalog}.{bronze_schema}.orders_stream")
        .dropDuplicates(["order_id"])
        .filter(col("order_id").isNotNull())
        .withColumn("order_date", to_date(col("order_date")))
    )