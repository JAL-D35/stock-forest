from datetime import date
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
    save_as_text,
)
from .holiday_checker import (
    Weekday,
    check_holiday,
    convert_to_date,
    get_holiday_date,
    get_holiday_info,
)


def load_holidays(input_date: date, private_key: str) -> Optional[List[str]]:
    holiday_url = (
        "http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo"
    )
    holiday_params = generate_url_params(
        serviceKey=private_key,
        solYear=input_date.strftime("%Y"),
        solMonth=input_date.strftime("%m"),
    )
    holiday_xml = get_data(holiday_url, holiday_params)

    if not holiday_xml:
        return

    holiday_dict = data_to_dict(holiday_xml)
    holiday_data = get_holiday_info(holiday_dict)

    if not holiday_data:
        return

    holidays = get_holiday_date(holiday_data)
    return holidays


def check_holiday_weekend(input_date: date, private_key: str) -> bool:
    if Weekday.is_weekend(input_date):
        return True

    holidays = load_holidays(input_date, private_key)

    if not holidays:  # If there is no public holiday
        return False  # must be a weekday

    return check_holiday(holidays, input_date.strftime("%Y%m%d"))


def load_stock(
    input_date: date,
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
        basDt=input_date.strftime("%Y%m%d"),
    )

    stocks_xml = get_data(url, params)

    if not stocks_xml:
        return

    stocks_dict = data_to_dict(stocks_xml)
    stocks_data = dict_to_json(stocks_dict)

    if not stocks_data:  # If there is no data
        return

    spark = SparkSession.builder.appName("stock_info").getOrCreate()
    stocks_rdd = convert_to_rdd(spark, stocks_data)
    stocks_df = convert_to_df(spark, stocks_rdd)
    return stocks_df


def run(
    input_date: str,
    private_key: str,
    result_type: str,
    market_class: str,
    n_rows: str,
    page_no: str,
    output_format: str,
    output_dir: str,
) -> None:
    converted_date = convert_to_date(input_date)

    if check_holiday_weekend(converted_date, private_key):
        return

    stocks_df = load_stock(
        input_date=converted_date,
        private_key=private_key,
        result_type=result_type,
        market_class=market_class,
        n_rows=n_rows,
        page_no=page_no,
    )

    if not stocks_df:  # If there is no data
        return

    clean_output_dir(market_class, output_dir)
    if output_format == "text":
        save_as_text(stocks_df, market_class, output_dir)
    save_as_parquet(stocks_df, market_class, output_dir)
