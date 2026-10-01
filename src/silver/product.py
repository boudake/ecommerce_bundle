from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.materialized_view(name="dev_bundle.silver.products")
def silver_products():

    return (
        spark.read.table("dev_bundle.bronze.products_stream")
        .dropDuplicates(["product_id"])
        .filter(col("product_id").isNotNull())
    )
