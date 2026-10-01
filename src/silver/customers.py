from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.materialized_view(name="dev_bundle.silver.customers")
def silver_customers():

    return (
        spark.read.table("dev_bundle.bronze.customers_stream")
        .dropDuplicates(["customer_id"])
        .filter(col("customer_id").isNotNull())
    )