from pyspark import pipelines as dp

@dp.table(name="customers_stream")
def bronze_customers_stream():

    raw_path = spark.conf.get("ecommerce.raw_path")
    customers_path = f"{raw_path}/customers"

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(customers_path)
    )