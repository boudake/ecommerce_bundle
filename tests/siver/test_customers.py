import pytest
import src.silver.clean_customer as clean_customer



def test_clean_customers(spark):
    data = [
        (1, "Alice"),
        (1, "Alice"),
        (None, "Bob")
    ]

    df = spark.createDataFrame(
        data,
        ["customer_id", "name"]
    )

    result = clean_customer(df)

    assert result.count() == 1