from src.silver.clean_customer import clean_customers


def test_clean_customers(spark):
    data = [
        (1, "Alice"),
        (1, "Alice"),
        (None, "Bob"),
    ]

    df = spark.createDataFrame(data, ["customer_id", "name"])

    result = clean_customers(df)

    assert result.count() == 1