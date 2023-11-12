from datetime import date
from typing import Optional


def is_weekend(input_date: str) -> bool:
    return input_date.weekday() > 4


def convert_to_date(date_str: str) -> date:
    return date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:]))


def get_holiday_info(data: dict) -> Optional[list]:
    try:
        return data["response"]["body"]["items"]["item"]
    except (KeyError, TypeError):
        return


def get_holiday_date(data: list) -> Optional[list]:
    return [holiday["locdate"] for holiday in data]


def check_holiday(holidays: list, input_date: str) -> bool:
    return input_date in holidays
