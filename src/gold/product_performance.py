from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    countDistinct,
    sum,
    avg
)

catalog = spark.conf.get("ecommerce.catalog")
silver_schema = spark.conf.get("ecommerce.silver.schema")
schema = spark.conf.get("ecommerce.gold.schema")

@dp.materialized_view(
    name=f"{catalog}.{schema}.product_performance"
)
def product_performance():

    orders = spark.read.table(
        f"{catalog}.{silver_schema}.orders"
    )

    products = spark.read.table(
        f"{catalog}.{silver_schema}.products"
    )

    categories = spark.read.table(
        f"{catalog}.{silver_schema}.categories"
    )

    return (
        orders
        .join(
            products,
            orders.product_id == products.product_id,
            "left"
        )
        .join(
            categories,
            products.category_id == categories.category_id,
            "left"
        )
        .groupBy(
            products.product_id,
            products.product_name,
            categories.category_name
        )
        .agg(
            countDistinct("order_id").alias("orders_count"),
            sum("quantity").alias("quantity_sold"),
            sum(
                col("quantity") * col("price")
            ).alias("revenue"),
            avg("price").alias("average_selling_price"),
            countDistinct("customer_id").alias(
                "unique_customers"
            )
        )
    )