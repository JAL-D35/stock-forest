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


def generate_url_params(*_: P.args, **kwargs: P.kwargs) -> Dict[str, Any]:
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


def clean_output_dir(mrkt_cls: str, dir_name: str) -> None:
    if exists(f"./{mrkt_cls}_{dir_name}"):
        rmtree(f"./{mrkt_cls}_{dir_name}")


def save_as_text(df: DataFrame, mrkt_cls: str, dir_name: str) -> None:
    df.write.json(f"./{mrkt_cls}_{dir_name}")


def save_as_parquet(df: DataFrame, mrkt_cls: str, dir_name: str) -> None:
    df.write.parquet(f"./{mrkt_cls}_{dir_name}")


def test_ci() -> str:
    string_variable: int = "string variable"
    return string_variable
