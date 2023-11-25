from datetime import date, datetime
from enum import IntEnum
from typing import Any, Dict, List, Optional


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


def get_holiday_info(data: Dict[str, Any]) -> Optional[List[Dict[str, str]]]:
    try:
        return data["response"]["body"]["items"]["item"]
    except (KeyError, TypeError):
        return


def get_holiday_date(data: List[Dict[str, str]]) -> List[str]:
    return [holiday["locdate"] for holiday in data]


def check_holiday(holidays: List[str], input_date: str) -> bool:
    return input_date in holidays
