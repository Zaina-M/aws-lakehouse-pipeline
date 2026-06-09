from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column


def null_in(*columns: str) -> Column:
    conditions = [F.col(c).isNull() for c in columns]
    return (
        conditions[0]
        if len(conditions) == 1
        else F.when(conditions[0], True).otherwise(
            null_in(*columns[1:]) if len(columns) > 1 else F.lit(False)
        )
    )


def _any_null(*columns: str) -> Column:
    cols = [F.col(c).isNull().cast("int") for c in columns]
    if len(cols) == 1:
        return cols[0].cast("boolean")
    return F.greatest(*cols).cast("boolean")


def negative_value(column: str) -> Column:
    return F.col(column) < 0


def future_date(column: str) -> Column:
    return F.col(column) > F.current_date()


def apply_validations(
    df: DataFrame, rules: list[tuple[Column, str]]
) -> tuple[DataFrame, DataFrame]:
    rejection_col = "_rejection_reason"
    working = df.withColumn(rejection_col, F.lit(None).cast("string"))

    for condition, reason in rules:
        working = working.withColumn(
            rejection_col,
            F.when(F.col(rejection_col).isNull() & condition, F.lit(reason)).otherwise(
                F.col(rejection_col)
            ),
        )

    valid = working.filter(F.col(rejection_col).isNull()).drop(rejection_col)
    rejected = working.filter(F.col(rejection_col).isNotNull())
    return valid, rejected


def check_referential_integrity(
    df: DataFrame,
    parent_df: DataFrame,
    fk_col: str,
    pk_col: str,
    rejection_reason: str,
) -> tuple[DataFrame, DataFrame]:
    parent_keys = parent_df.select(F.col(pk_col).alias("__pk")).distinct()
    valid = df.join(parent_keys, F.col(fk_col) == F.col("__pk"), "left_semi")
    rejected = df.join(
        parent_keys, F.col(fk_col) == F.col("__pk"), "left_anti"
    ).withColumn("_rejection_reason", F.lit(rejection_reason))
    return valid, rejected
