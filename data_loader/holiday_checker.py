from datetime import date, datetime
from enum import IntEnum
from typing import Any, Dict, List, Optional, Union


class Weekday(IntEnum):
    MONDAY = 0
    TUESDAY = 1
    WEDNESDAY = 2
    THURSDAY = 3
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6

    @classmethod
    def is_weekend(cls, input_date: date) -> bool:
        return cls(input_date.weekday()) > cls.FRIDAY


def convert_to_date(date_str: str) -> date:
    if len(date_str) != 8 or not date_str.isdecimal():
        raise ValueError("Invalid date format")

    return datetime.strptime(date_str, "%Y%m%d").date()


def get_holiday_info(
    data: Dict[str, Any]
) -> Optional[Union[List[Dict[str, str]], Dict[str, str]]]:
    try:
        return data["response"]["body"]["items"]["item"]
    except (KeyError, TypeError):
        return


def check_key_in_dict(input_dict: Dict[str, Any], key: str) -> bool:
    return key in input_dict


def get_holiday_date(
    data: Union[List[Dict[str, str]], Dict[str, str]]
) -> Optional[List[str]]:
    if isinstance(data, dict):
        if check_key_in_dict(data, "locdate"):
            return [data["locdate"]]
        return
    return [
        holiday["locdate"] for holiday in data if check_key_in_dict(holiday, "locdate")
    ]


def check_holiday(holidays: List[str], input_date: str) -> bool:
    return input_date in holidays
