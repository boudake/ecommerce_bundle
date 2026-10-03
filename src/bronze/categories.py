from pyspark import pipelines as dp

catalog = spark.conf.get("ecommerce.catalog")
schema = spark.conf.get("ecommerce.bronze.schema")
raw_path = spark.conf.get("ecommerce.raw_path")
categories_path = f"{raw_path}/category"
@dp.table(name=f"{catalog}.{schema}.categories_stream")
def bronze_categories_stream():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(categories_path)
    )