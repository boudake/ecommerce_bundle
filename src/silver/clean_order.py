from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date


def clean_orders(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["order_id"])
          .filter(col("amount").isNotNull() & (col("amount") > 0))
          .withColumn("order_date", to_date(col("order_ts")))
    )
