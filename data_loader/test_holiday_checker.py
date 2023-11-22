from datetime import datetime, timedelta
from random import randint
from typing import Optional

import pytest

from .holiday_checker import Weekday, convert_to_date

BASE_DATETIME = datetime(randint(0, 2023), randint(1, 12), randint(1, 31))


@pytest.mark.parametrize(
    "input_date",
    [
        BASE_DATETIME,
        BASE_DATETIME + timedelta(days=1),
        BASE_DATETIME + timedelta(days=2),
        BASE_DATETIME + timedelta(days=3),
        BASE_DATETIME + timedelta(days=4),
        BASE_DATETIME + timedelta(days=5),
        BASE_DATETIME + timedelta(days=6),
    ],
)
def test_is_weekend(input_date: datetime) -> None:
    assert Weekday.is_weekend(input_date) == (input_date.weekday() > 4)  # 4 is Friday


def test_convert_to_date() -> None:
    input_date = BASE_DATETIME.strftime("%Y%m%d")  # e.g. 20210101
    expected = BASE_DATETIME.date()
    assert convert_to_date(input_date) == expected
