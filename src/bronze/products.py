from pyspark import pipelines as dp

@dp.table(name="products_stream")
def bronze_products_stream():

    raw_path = spark.conf.get("ecommerce.raw_path")
    products_path = f"{raw_path}/products"

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(products_path)
    )