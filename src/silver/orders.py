from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date

@dp.materialized_view(name="dev_bundle.silver.orders")
def silver_orders():

    return (
        spark.read.table("dev_bundle.bronze.orders_stream")
        .dropDuplicates(["order_id"])
        .filter(col("order_id").isNotNull())
        .withColumn("order_date", to_date(col("order_date")))
    )