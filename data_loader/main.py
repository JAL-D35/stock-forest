from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession

from .data_loader import (
    clean_output_dir,
    convert_to_df,
    convert_to_rdd,
    data_to_dict,
    dict_to_json,
    generate_url_params,
    get_data,
    save_as_parquet,
)
from .holiday_checker import (
    Weekday,
    check_holiday,
    convert_to_date,
    get_holiday_date,
    get_holiday_info,
)


def load_holidays(input_date: str, private_key: str) -> Optional[List[str]]:
    converted_date = convert_to_date(input_date)
    holiday_url = (
        "http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo"
    )
    holiday_params = generate_url_params(
        serviceKey=private_key,
        solYear=converted_date.strftime("%Y"),
        solMonth=converted_date.strftime("%m"),
    )
    holiday_data = get_data(holiday_url, holiday_params)
    if not holiday_data:
        return

    holiday_data = data_to_dict(holiday_data)
    holiday_data = get_holiday_info(holiday_data)

    if not holiday_data:
        return
    holiday_data = get_holiday_date(holiday_data)
    return holiday_data


def check_holiday_weekend(input_date: str, private_key: str) -> bool:
    if Weekday.is_weekend(convert_to_date(input_date)):
        return True

    holiday_data = load_holidays(input_date, private_key)
    if not holiday_data:
        return False
    return check_holiday(holiday_data, input_date)


def load_stock(
    input_date: str,
    private_key: str,
    result_type: str,
    market_class: str,
    n_rows: str,
    page_no: str,
) -> Optional[DataFrame]:
    url = "http://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo"
    params = generate_url_params(
        serviceKey=private_key,
        resultType=result_type,
        mrktCls=market_class,
        numOfRows=n_rows,
        pageNo=page_no,
        basDt=input_date,
    )

    data = get_data(url, params)
    if not data:
        return

    data = data_to_dict(data)
    data = dict_to_json(data)
    if not data:
        return
    spark = SparkSession.builder.appName("stock_info").getOrCreate()
    rdd = convert_to_rdd(spark, data)
    df = convert_to_df(spark, rdd)
    return df


def run(
    input_date: str,
    private_key: str,
    result_type: str,
    market_class: str,
    n_rows: str,
    page_no: str,
) -> None:
    if check_holiday_weekend(input_date, private_key):
        return

    df = load_stock(input_date, private_key, result_type, market_class, n_rows, page_no)
    if not df:
        return

    clean_output_dir(market_class, input_date)

    save_as_parquet(df, market_class, input_date)
