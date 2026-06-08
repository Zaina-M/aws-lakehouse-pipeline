from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType, DateType

from utils.validators import apply_validations, _any_null, negative_value, future_date
from utils.delta_ops import upsert
from utils.s3_ops import list_keys, read_excel, archive, write_rejects


# Matches the real orders XLSX: order_num, order_id, user_id, order_timestamp, total_amount, date
_SCHEMA_CASTS = {
    "order_num": IntegerType(),
    "order_id": IntegerType(),
    "user_id": IntegerType(),
    "order_timestamp": TimestampType(),
    "total_amount": DoubleType(),
    "date": DateType(),
}

# Built lazily inside run(): these call F.col(), which requires an active
# SparkContext. main.py imports this module before SparkContext() exists, so
# building them at module level raises AssertionError.
def _validation_rules():
    return [
        (_any_null("order_id", "user_id", "date"), "null_required_field"),
        (negative_value("total_amount"), "negative_total_amount"),
        (future_date("date"), "future_order_date"),
    ]


def run(
    spark: SparkSession,
    raw_bucket: str,
    dwh_bucket: str,
    archive_bucket: str,
    reject_bucket: str,
    database: str,
    run_date: str,
) -> None:
    keys = list_keys(raw_bucket, "orders/")
    if not keys:
        return

    for key in keys:
        df = read_excel(spark, raw_bucket, key)

        for col_name, dtype in _SCHEMA_CASTS.items():
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))

        df = df.dropDuplicates(["order_id"])

        valid, rejected = apply_validations(df, _validation_rules())

        delta_path = f"s3://{dwh_bucket}/orders/"
        upsert(spark, valid, delta_path, merge_keys=["order_id"], partition_cols=["date"])

        write_rejects(rejected, reject_bucket, "orders", run_date)
        archive(raw_bucket, key, archive_bucket, run_date)
