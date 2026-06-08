from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType, DateType

from utils.validators import apply_validations, _any_null, check_referential_integrity
from utils.delta_ops import upsert
from utils.s3_ops import list_keys, read_excel, archive, write_rejects

# Matches the real order_items XLSX: id, order_id, user_id, days_since_prior_order,
# product_id, add_to_cart_order, reordered, order_timestamp, date
_SCHEMA_CASTS = {
    "id": IntegerType(),
    "order_id": IntegerType(),
    "user_id": IntegerType(),
    "days_since_prior_order": IntegerType(),
    "product_id": IntegerType(),
    "add_to_cart_order": IntegerType(),
    "reordered": IntegerType(),
    "order_timestamp": TimestampType(),
    "date": DateType(),
}


# Built lazily inside run(): these call F.col(), which requires an active
# SparkContext. main.py imports this module before SparkContext() exists, so
# building them at module level raises AssertionError.
def _validation_rules():
    return [
        (_any_null("id", "order_id", "product_id"), "null_required_field"),
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
    keys = list_keys(raw_bucket, "order_items/")
    if not keys:
        return

    orders_path = f"s3://{dwh_bucket}/orders/"
    products_path = f"s3://{dwh_bucket}/products/"
    orders_df = spark.read.format("delta").load(orders_path).select("order_id")
    products_df = spark.read.format("delta").load(products_path).select("product_id")

    for key in keys:
        df = read_excel(spark, raw_bucket, key)

        for col_name, dtype in _SCHEMA_CASTS.items():
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))

        df = df.dropDuplicates(["id"])

        valid, rejected = apply_validations(df, _validation_rules())

        valid, orphan_orders = check_referential_integrity(
            valid, orders_df, "order_id", "order_id", "orphan_order_id"
        )
        valid, orphan_products = check_referential_integrity(
            valid, products_df, "product_id", "product_id", "orphan_product_id"
        )

        all_rejected = rejected.unionByName(
            orphan_orders, allowMissingColumns=True
        ).unionByName(orphan_products, allowMissingColumns=True)

        delta_path = f"s3://{dwh_bucket}/order_items/"
        upsert(spark, valid, delta_path, merge_keys=["id"], partition_cols=["date"])

        write_rejects(all_rejected, reject_bucket, "order_items", run_date)
        archive(raw_bucket, key, archive_bucket, run_date)
