import io
import boto3
import pandas as pd
from pyspark.sql import SparkSession, DataFrame

_s3 = boto3.client("s3")


def list_keys(bucket: str, prefix: str) -> list[str]:
    keys = []
    paginator = _s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def _get_bytes(bucket: str, key: str) -> bytes:
    response = _s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def _nan_to_none(pdf: "pd.DataFrame") -> "pd.DataFrame":
    # Pandas represents missing values as NaN/NaT, which Spark does NOT treat as
    # SQL NULL — that silently defeats the null-required-field validations. Convert
    # every missing value to Python None so Spark reads them as genuine nulls.
    return pdf.astype(object).where(pd.notnull(pdf), None)


def read_csv(spark: SparkSession, bucket: str, key: str) -> DataFrame:
    # Read every column as text and treat only empty cells as missing. The job's
    # explicit schema casts handle typing, and null detection stays correct.
    pdf = pd.read_csv(
        io.BytesIO(_get_bytes(bucket, key)),
        dtype=str,
        keep_default_na=False,
        na_values=[""],
    )
    return spark.createDataFrame(_nan_to_none(pdf))


def read_excel(
    spark: SparkSession, bucket: str, key: str, sheet_name: int | str = 0
) -> DataFrame:
    pdf = pd.read_excel(
        io.BytesIO(_get_bytes(bucket, key)), sheet_name=sheet_name, engine="openpyxl"
    )
    return spark.createDataFrame(_nan_to_none(pdf))


def archive(src_bucket: str, src_key: str, archive_bucket: str, run_date: str) -> None:
    filename = src_key.split("/")[-1]
    dest_key = f"archived/{run_date}/{filename}"
    _s3.copy_object(
        CopySource={"Bucket": src_bucket, "Key": src_key},
        Bucket=archive_bucket,
        Key=dest_key,
    )
    _s3.delete_object(Bucket=src_bucket, Key=src_key)


def write_rejects(
    df: DataFrame, reject_bucket: str, dataset: str, run_date: str
) -> None:
    if df.count() == 0:
        return
    dest = f"s3://{reject_bucket}/rejects/{dataset}/{run_date}/"
    df.write.mode("overwrite").option("header", True).csv(dest)
