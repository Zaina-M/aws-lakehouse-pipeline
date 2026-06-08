from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

from utils.validators import apply_validations, _any_null
from utils.delta_ops import upsert
from utils.s3_ops import list_keys, read_csv, archive, write_rejects


# Matches the real products.csv: product_id, department_id, department, product_name
_SCHEMA_CASTS = {
    "product_id": IntegerType(),
    "department_id": IntegerType(),
    "department": StringType(),
    "product_name": StringType(),
}

# Built lazily inside run(): these call F.col(), which requires an active
# SparkContext. main.py imports this module before SparkContext() exists, so
# building them at module level raises AssertionError.
def _validation_rules():
    return [
        (_any_null("product_id", "product_name"), "null_required_field"),
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
    keys = list_keys(raw_bucket, "products/")
    if not keys:
        return

    for key in keys:
        df = read_csv(spark, raw_bucket, key)

        for col_name, dtype in _SCHEMA_CASTS.items():
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))

        df = df.dropDuplicates(["product_id"])

        valid, rejected = apply_validations(df, _validation_rules())

        delta_path = f"s3://{dwh_bucket}/products/"
        upsert(spark, valid, delta_path, merge_keys=["product_id"])

        write_rejects(rejected, reject_bucket, "products", run_date)
        archive(raw_bucket, key, archive_bucket, run_date)
