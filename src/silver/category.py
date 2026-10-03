from pyspark import pipelines as dp
from pyspark.sql.functions import col


catalog = spark.conf.get("ecommerce.catalog")
schema = spark.conf.get("ecommerce.silver.schema")
bronze_schema = spark.conf.get("ecommerce.bronze.schema")

@dp.materialized_view(name=f"{catalog}.{schema}.categories")
def silver_categories():

    return (
        spark.read.table(f"{catalog}.{bronze_schema}.categories_stream")
        .dropDuplicates(["category_id"])
        .filter(col("category_id").isNotNull())
    )