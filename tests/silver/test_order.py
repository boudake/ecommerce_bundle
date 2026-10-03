
from pyspark.testing import assertDataFrameEqual
from src.silver.clean_order import clean_orders

def test_clean_orders_dedups_and_drops_non_positive(spark):
    src = spark.createDataFrame(
        [(1, 10.0, "2026-01-01 10:00:00"),
         (1, 10.0, "2026-01-01 10:00:00"),   # duplicate
         (2, -5.0, "2026-01-02 11:00:00")],  # invalid amount
        "order_id INT, amount DOUBLE, order_ts STRING",
    )
    out = clean_orders(src).select("order_id", "amount")
    expected = spark.createDataFrame([(1, 10.0)], "order_id INT, amount DOUBLE")
    assertDataFrameEqual(out, expected)