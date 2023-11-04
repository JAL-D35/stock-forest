from json import dumps
from os.path import exists
from shutil import rmtree

from pyspark.sql import SparkSession
from requests import get
from xmltodict import parse


def generate_url_params(**kwargs) -> dict:
    params = {}
    params.update(kwargs)
    return params


def get_data(url: str, params: dict):
    return get(url, params=params).text


def data_to_dict(data: str):
    return parse(data)


def dict_to_json(data: dict):
    try:
        return [dumps(data["response"]["body"]["items"]["item"], indent=4)]
    except (KeyError, TypeError):
        return


def convert_to_rdd(spark: SparkSession, info_list: list):
    return spark.sparkContext.parallelize(info_list)


def convert_to_df(spark: SparkSession, rdd):
    return spark.read.json(rdd)


def check_output_dir(mrkt_cls: str, dir_name: str):
    if exists(f"./{mrkt_cls}_{dir_name}"):
        rmtree(f"./{mrkt_cls}_{dir_name}")


def save_as_text(df: DataFrame, mrkt_cls: str, dir_name: str):
    df.write.json(f"./{mrkt_cls}_{dir_name}")
