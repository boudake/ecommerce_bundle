from pyspark import pipelines as dp

from silver.clean_order import clean_orders

catalog = spark.conf.get("ecommerce.catalog")
schema = spark.conf.get("ecommerce.silver.schema")
bronze_schema = spark.conf.get("ecommerce.bronze.schema")
@dp.table(name=f"{catalog}.{schema}.orders")
def silver_orders():

    df =  spark.read.table(f"{catalog}.{bronze_schema}.orders_stream")
    return clean_orders(df)
     