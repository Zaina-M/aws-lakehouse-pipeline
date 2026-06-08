from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable


def upsert(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    merge_keys: list[str],
    partition_cols: list[str] | None = None,
) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        dt = DeltaTable.forPath(spark, path)
        match_condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
        dt.alias("target").merge(
            df.alias("source"), match_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        writer = df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)
