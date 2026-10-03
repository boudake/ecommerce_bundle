from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def clean_customers(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )