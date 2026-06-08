import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from jobs import products, orders, order_items


_REQUIRED_ARGS = [
    "JOB_NAME",
    "DATASET_TYPE",
    "RAW_BUCKET",
    "DWH_BUCKET",
    "ARCHIVE_BUCKET",
    "REJECT_BUCKET",
    "DATABASE_NAME",
    "RUN_DATE",
]

_HANDLERS = {
    "products": products.run,
    "orders": orders.run,
    "order_items": order_items.run,
}


def main() -> None:
    args = getResolvedOptions(sys.argv, _REQUIRED_ARGS)

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # The readers build Spark DataFrames from pandas. Arrow's pandas conversion is
    # brittle with object columns containing None and with datetime/NaT values, so
    # use the row-based path which handles them reliably.
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    dataset_type = args["DATASET_TYPE"]
    handler = _HANDLERS.get(dataset_type)
    if handler is None:
        raise ValueError(f"Unknown DATASET_TYPE: {dataset_type!r}. Must be one of {list(_HANDLERS)}")

    handler(
        spark=spark,
        raw_bucket=args["RAW_BUCKET"],
        dwh_bucket=args["DWH_BUCKET"],
        archive_bucket=args["ARCHIVE_BUCKET"],
        reject_bucket=args["REJECT_BUCKET"],
        database=args["DATABASE_NAME"],
        run_date=args["RUN_DATE"],
    )

    job.commit()


if __name__ == "__main__":
    main()
