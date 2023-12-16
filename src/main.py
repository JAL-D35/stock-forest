from datetime import date, timedelta
from typing import Generator, List, Optional

from pyspark.sql import DataFrame, SparkSession

from data_loader import (
    change_dtype,
    clean_output_dir,
    concat_dataframes,
    convert_to_df,
    convert_to_rdd,
    data_to_dict,
    dict_to_json,
    generate_params,
    get_data,
    save_dataframe,
)
from holiday_checker import (
    Weekday,
    check_holiday,
    convert_to_date,
    get_holiday_date,
    get_holiday_info,
)

DATE_FORMAT = "%Y%m%d"


def load_holidays(input_date: date, private_key: str) -> Optional[List[str]]:
    holiday_url = (
        "http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo"
    )
    holiday_params = generate_params(
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

    return check_holiday(holidays, input_date.strftime(DATE_FORMAT))


def calculate_delta_days(start_date: date, end_date: date) -> Optional[int]:
    if end_date > start_date:
        return (end_date - start_date).days


def target_date_generator(start_date: date, delta: int) -> Generator[date, None, None]:
    for days in range(delta + 1):
        yield start_date + timedelta(days=days)


def load_stock(
    input_date: date,
    private_key: str,
    result_type: str,
    market_class: str,
    n_rows: str,
    page_no: str,
) -> Optional[DataFrame]:
    url = "http://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo"
    params = generate_params(
        serviceKey=private_key,
        resultType=result_type,
        mrktCls=market_class,
        numOfRows=n_rows,
        pageNo=page_no,
        basDt=input_date.strftime(DATE_FORMAT),
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
    start_date: str,
    end_date: str,
    private_key: str,
    result_type: str,
    market_class: str,
    n_rows: str,
    page_no: str,
    output_format: str,
    output_dir: str,
) -> None:
    converted_start_date = convert_to_date(start_date)
    converted_end_date = convert_to_date(end_date)
    days = calculate_delta_days(converted_start_date, converted_end_date)

    if days is None:
        return

    dataframes = []

    for target_date in target_date_generator(converted_start_date, days):
        if not check_holiday_weekend(target_date, private_key):
            stocks_df = load_stock(
                input_date=target_date,
                private_key=private_key,
                result_type=result_type,
                market_class=market_class,
                n_rows=n_rows,
                page_no=page_no,
            )

            if stocks_df:
                dataframes.append(stocks_df)

    clean_output_dir(market_class, output_dir)

    dtype_dict = generate_params(
        clpr="float",
        fltRt="float",
        hiprc="float",
        isinCd="string",
        itmsNm="string",
        lopr="float",
        lstgStCnt="int",
        mkp="float",
        mrktCtg="string",
        mrktTotAmt="float",
        crtnCd="string",
        trPrc="float",
        trqu="float",
        vs="float",
        basDt="string",
    )

    change_dataframes = [
        change_dtype(dataframe, dtype_dict) for dataframe in dataframes
    ]

    save_dataframe(
        dataframe=concat_dataframes(change_dataframes),
        file_format=output_format,
        partition_value="basDt",
        output_dir=output_dir,
    )
