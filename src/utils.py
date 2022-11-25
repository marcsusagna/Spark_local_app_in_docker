from pyspark.sql import DataFrame


def write_to_disk(df: DataFrame, output_dir: str, file_name: str, num_output_partitions = 1):
    """
    For this task it is rather simplistic, but generally there are reused steps when storing to disk
    :param df:
    :param output_dir:
    :return: None
    """
    df.repartition(num_output_partitions).write.\
        option("header", True).option("delimiter", "\t").mode("overwrite")\
        .csv(output_dir+file_name)