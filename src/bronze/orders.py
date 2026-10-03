from pyspark import pipelines as dp

catalog = spark.conf.get("ecommerce.catalog")
schema = spark.conf.get("ecommerce.bronze.schema")
raw_path = spark.conf.get("ecommerce.raw_path")
orders_path = f"{raw_path}/orders"
@dp.table(name=f"{catalog}.{schema}.orders_stream")

def bronze_orders_stream():    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load(orders_path)
    )