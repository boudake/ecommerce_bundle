from pyspark import pipelines as dp

catalog = spark.conf.get("ecommerce.catalog")
schema = spark.conf.get("ecommerce.bronze.schema")
raw_path = spark.conf.get("ecommerce.raw_path")
customers_path = f"{raw_path}/customers"
@dp.table(name=f"{catalog}.{schema}.customers_stream")
def bronze_customers_stream():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(customers_path)
    )