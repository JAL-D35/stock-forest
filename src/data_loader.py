from json import dumps
from os.path import exists
from shutil import rmtree
from typing import Any, Dict, Iterable, List, Optional

from pyre_extensions import ParameterSpecification
from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
from requests import get
from xmltodict import parse

P = ParameterSpecification("P")


def generate_params(*_: P.args, **kwargs: P.kwargs) -> Dict[str, Any]:
    params = {}
    params.update(kwargs)
    return params


def get_data(url: str, params: Dict[str, Any]) -> Optional[str]:
    response = get(url, params=params)
    if response.status_code == 200:
        return response.text


def data_to_dict(data: str) -> Dict[str, Any]:
    return parse(data)


def dict_to_json(data: Dict[str, Any]) -> Optional[List[str]]:
    try:
        return [dumps(data["response"]["body"]["items"]["item"], indent=4)]
    except (KeyError, TypeError):
        return


def convert_to_rdd(spark: SparkSession, info_list: Iterable[str]) -> RDD[str]:
    return spark.sparkContext.parallelize(info_list)


def convert_to_df(spark: SparkSession, rdd: RDD[str]) -> DataFrame:
    return spark.read.json(rdd)


def drop_duplicate(dataframe: DataFrame) -> DataFrame:
    return dataframe.dropDuplicates()


def concat_dataframes(dataframes: List[DataFrame]) -> DataFrame:
    output_df = dataframes[0]
    for dataframe in dataframes[1:]:
        output_df = output_df.union(dataframe)
    return drop_duplicate(output_df)  # drop duplicate rows


def clean_output_dir(mrkt_cls: str, dir_name: str) -> None:
    if exists(f"./{mrkt_cls}_{dir_name}"):
        rmtree(f"./{mrkt_cls}_{dir_name}")


def save_as_text(df: DataFrame, mrkt_cls: str, dir_name: str) -> None:
    df.write.json(f"./{mrkt_cls}_{dir_name}")


def save_as_parquet(df: DataFrame, mrkt_cls: str, dir_name: str) -> None:
    df.write.parquet(f"./{mrkt_cls}_{dir_name}")


def save_dataframe(
    dataframe: DataFrame, file_format: str, partition_value: str, output_dir: str
) -> None:
    dataframe.write.format(file_format).partitionBy(partition_value).save(
        output_dir, mode="append"
    )


def generate_dtype_dict(*_: P.args, **kwargs: P.kwargs) -> Dict[str, str]:
    dtype_dict = {}
    dtype_dict.update(kwargs)
    return dtype_dict


def change_dtype(dataframe: DataFrame, dtype_dict: Dict[str, str]) -> DataFrame:
    for column, dtype in dtype_dict.items():
        dataframe = dataframe.withColumn(column, dataframe[column].cast(dtype))
    return dataframe
