import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
)

from utils.validators import (
    _any_null,
    negative_value,
    future_date,
    apply_validations,
    check_referential_integrity,
)


def test_any_null_flags_null_pk(spark):
    df = spark.createDataFrame([(None, "a"), (1, "b")], ["id", "name"])
    result = df.withColumn("bad", _any_null("id"))
    rows = {r["id"]: r["bad"] for r in result.collect()}
    assert rows[None] is True
    assert rows[1] is False


def test_any_null_flags_when_any_column_null(spark):
    df = spark.createDataFrame([(1, None), (2, "x")], ["id", "name"])
    result = df.withColumn("bad", _any_null("id", "name"))
    rows = {r["id"]: r["bad"] for r in result.collect()}
    assert rows[1] is True
    assert rows[2] is False


def test_negative_value_flags_negatives(spark):
    df = spark.createDataFrame([(-1.0,), (0.0,), (5.0,)], ["price"])
    result = df.withColumn("bad", negative_value("price"))
    rows = {r["price"]: r["bad"] for r in result.collect()}
    assert rows[-1.0] is True
    assert rows[0.0] is False
    assert rows[5.0] is False


def test_future_date_flags_future(spark):
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    df = spark.createDataFrame(
        [(tomorrow,), (yesterday,)],
        StructType([StructField("order_date", DateType(), True)]),
    )
    result = df.withColumn("bad", future_date("order_date"))
    rows = {r["order_date"]: r["bad"] for r in result.collect()}
    assert rows[tomorrow] is True
    assert rows[yesterday] is False


def test_apply_validations_splits_correctly(spark):
    df = spark.createDataFrame([(None, 10.0), (1, -5.0), (2, 20.0)], ["id", "price"])
    rules = [
        (_any_null("id"), "null_id"),
        (negative_value("price"), "negative_price"),
    ]
    valid, rejected = apply_validations(df, rules)

    assert valid.count() == 1
    assert valid.collect()[0]["id"] == 2

    assert rejected.count() == 2
    reasons = {r["id"]: r["_rejection_reason"] for r in rejected.collect()}
    assert reasons[None] == "null_id"
    assert reasons[1] == "negative_price"


def test_apply_validations_first_rule_wins(spark):
    df = spark.createDataFrame([(None, -5.0)], ["id", "price"])
    rules = [
        (_any_null("id"), "null_id"),
        (negative_value("price"), "negative_price"),
    ]
    _, rejected = apply_validations(df, rules)
    assert rejected.collect()[0]["_rejection_reason"] == "null_id"


def test_check_referential_integrity(spark):
    child = spark.createDataFrame([(1, 100), (2, 999), (3, 100)], ["id", "order_id"])
    parent = spark.createDataFrame([(100,), (200,)], ["order_id"])

    valid, rejected = check_referential_integrity(
        child, parent, "order_id", "order_id", "orphan_order_id"
    )

    valid_ids = {r["id"] for r in valid.collect()}
    rejected_ids = {r["id"] for r in rejected.collect()}

    assert valid_ids == {1, 3}
    assert rejected_ids == {2}
    assert rejected.collect()[0]["_rejection_reason"] == "orphan_order_id"
