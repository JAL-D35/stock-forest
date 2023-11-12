from datetime import date, datetime
from typing import Any, Dict, List, Optional


def is_weekend(input_date: date) -> bool:
    return input_date.weekday() > 4


def convert_to_date(date_str: str) -> date:
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
