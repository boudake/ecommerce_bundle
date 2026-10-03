from pyspark import pipelines as dp
from pyspark.sql.functions import col


catalog = spark.conf.get("ecommerce.catalog")
schema = spark.conf.get("ecommerce.silver.schema")
bronze_schema = spark.conf.get("ecommerce.bronze.schema")
@dp.table(name=f"{catalog}.{schema}.products")
def silver_products():

    return (
        spark.read.table(f"{catalog}.{bronze_schema}.products_stream")
        .dropDuplicates(["product_id"])
        .filter(col("product_id").isNotNull())
    )
