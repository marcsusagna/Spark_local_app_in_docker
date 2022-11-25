from pyspark.sql import (
    DataFrame,
    functions as F,
    types as T
)

from src.data_health_checks.utils import handle_check_behavior


def null_check(df: DataFrame, columns_to_check: list, except_on_fail=True):
    """
    Assumes null values come as null (i.e. don't have special string codings). This needs to be assessed when
    exploring the dataset. These checks are based on assumptions on the input dataset
    """
    # Using select based on list comprehension instead of a for loop on each column to avoid confusing the query plan
    #   (based on spark best practices)
    # Using rdd.isEmpty instead of count>0 because it is faster (less work to be done)
    null_indicators = [F.when(F.col(x).isNull(), F.lit(1)).otherwise(F.lit(0)).alias(x) for x in columns_to_check]
    sum_expression = "+".join(columns_to_check)
    null_check_passed = (
        df
        .select(null_indicators)
        .withColumn("has_any_na", F.expr(sum_expression))
        .where(F.col("has_any_na") > 0)
        .rdd
        .isEmpty()
    )
    check_name = "Null checks"
    handle_check_behavior(
        check_boolean=null_check_passed,
        except_on_fail=except_on_fail,
        check_name=check_name
    )


def uniqueness_check(df: DataFrame, columns_to_check: list, except_on_fail=True):
    pk_check_passed = (
        df
        .groupBy(columns_to_check)
        # No need to use LongType, a candidate pk would present row counts close to 1
        .agg(F.count("*").cast(T.IntegerType()).alias("row_count"))
        .where(F.col("row_count") > 1)
        .rdd
        .isEmpty()
    )
    check_name = "Uniqueness check"
    handle_check_behavior(
        check_boolean=pk_check_passed,
        except_on_fail=except_on_fail,
        check_name=check_name
    )


def invalid_string(df: DataFrame, columns_to_check: list, invalid_string: str, except_on_fail=True):
    indicator_cols = [F.when(F.col(x).contains(invalid_string), F.lit(1)).otherwise(F.lit(0)).alias(x) for x in columns_to_check]
    sum_expression = "+".join(columns_to_check)
    string_check_passed = (
        df
        .select(indicator_cols)
        .withColumn("has_any_invalid_string", F.expr(sum_expression))
        .where(F.col("has_any_invalid_string") > 0)
        .rdd
        .isEmpty()
    )
    check_name = "Checking for invalid string {}".format(invalid_string)
    handle_check_behavior(
        check_boolean=string_check_passed,
        except_on_fail=except_on_fail,
        check_name=check_name
    )


def timestamp_format_check(df: DataFrame, column_to_check:str, expected_format="yyyy-MM-dd'T'HH:mm:ss'Z'", except_on_fail=True):
    timestamp_format_passed = (
        df
        .withColumn(column_to_check, F.to_timestamp(F.col(column_to_check), expected_format))
        .where(F.col(column_to_check).isNull())
        .rdd
        .isEmpty()
    )
    check_name = "Checking if column {} is correctly formatted".format(column_to_check)
    handle_check_behavior(
        check_boolean=timestamp_format_passed,
        except_on_fail=except_on_fail,
        check_name=check_name
    )