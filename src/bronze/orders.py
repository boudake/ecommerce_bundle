from pyspark import pipelines as dp

@dp.table(name="dev_bundle.bronze.orders_stream")
def bronze_orders_stream():
    
    
    raw_path = spark.conf.get("ecommerce.raw_path")
    orders_path = f"{raw_path}/orders"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load(orders_path)
    )