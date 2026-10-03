from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    countDistinct,
    sum,
    avg,
    max,
    min,
    datediff,
    current_date,
    when,
    coalesce,
    lit
)

catalog = spark.conf.get("ecommerce.catalog")
silver_schema = spark.conf.get("ecommerce.silver.schema")
schema = spark.conf.get("ecommerce.gold.schema")

@dp.materialized_view(
    name=f"{catalog}.{schema}.customer_360"
)
def customer_360():

    customers = (
        spark.read
        .table(f"{catalog}.{silver_schema}.customers")
    )

    orders = (
        spark.read
        .table(f"{catalog}.{silver_schema}.orders")
    )

    # ------------------------------------------------
    # Customer purchasing metrics
    # ------------------------------------------------

    customer_metrics = (
        orders
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id")
                .alias("total_orders"),

            sum("quantity")
                .alias("total_items"),

            sum(
                col("quantity") * col("unit_price")
            ).alias("total_spend"),

            avg(
                col("quantity") * col("unit_price")
            ).alias("average_order_value"),

            min("order_date")
                .alias("first_order_date"),

            max("order_date")
                .alias("last_order_date")
        )
    )

    # ------------------------------------------------
    # Customer 360
    # ------------------------------------------------

    return (
        customers
        .join(
            customer_metrics,
            customers.customer_id
            == customer_metrics.customer_id,
            "left"
        )
        .drop(customers.customer_id)

        # Customers who never purchased
        .withColumn(
            "total_orders",
            coalesce(col("total_orders"), lit(0))
        )

        .withColumn(
            "total_items",
            coalesce(col("total_items"), lit(0))
        )

        .withColumn(
            "total_spend",
            coalesce(col("total_spend"), lit(0))
        )

        .withColumn(
            "average_order_value",
            coalesce(
                col("average_order_value"),
                lit(0)
            )
        )

        # ------------------------------------------------
        # Recency
        # ------------------------------------------------

        .withColumn(
            "days_since_last_order",
            when(
                col("last_order_date").isNotNull(),
                datediff(
                    current_date(),
                    col("last_order_date")
                )
            )
        )

        # ------------------------------------------------
        # Customer segmentation
        # ------------------------------------------------

        .withColumn(
            "customer_segment",
            when(
                col("total_orders") == 0,
                "NO_PURCHASE"
            )
            .when(
                col("total_spend") >= 20000,
                "VIP"
            )
            .when(
                col("total_spend") >= 10000,
                "HIGH_VALUE"
            )
            .when(
                col("total_spend") >= 1500,
                "MEDIUM_VALUE"
            )
            .otherwise(
                "LOW_VALUE"
            )
        )
    )