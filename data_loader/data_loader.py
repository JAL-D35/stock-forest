from json import dumps
from os.path import exists
from shutil import rmtree
from typing import Iterable, Optional

from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
from requests import get
from xmltodict import parse


def generate_url_params(**kwargs) -> dict:
    params = {}
    params.update(kwargs)
    return params


def get_data(url: str, params: dict) -> str:
    return get(url, params=params).text


def data_to_dict(data: str) -> dict:
    return parse(data)


def dict_to_json(data: dict) -> Optional[list]:
    try:
        return [dumps(data["response"]["body"]["items"]["item"], indent=4)]
    except (KeyError, TypeError):
        return


def convert_to_rdd(spark: SparkSession, info_list: Iterable[str]) -> RDD[str]:
    return spark.sparkContext.parallelize(info_list)


def convert_to_df(spark: SparkSession, rdd: RDD[str]) -> DataFrame:
    return spark.read.json(rdd)


def check_output_dir(mrkt_cls: str, dir_name: str) -> None:
    if exists(f"./{mrkt_cls}_{dir_name}"):
        rmtree(f"./{mrkt_cls}_{dir_name}")


def save_as_text(df: DataFrame, mrkt_cls: str, dir_name: str) -> None:
    df.write.json(f"./{mrkt_cls}_{dir_name}")
