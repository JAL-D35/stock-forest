from datetime import date, datetime, timedelta
from random import randint
from typing import Optional

import pytest

from .holiday_checker import Weekday, convert_to_date

BASE_DATE = datetime(randint(0, 2023), randint(1, 12), randint(1, 31))


@pytest.mark.parametrize(
    "input_date",
    [
        BASE_DATE,
        BASE_DATE + timedelta(days=1),
        BASE_DATE + timedelta(days=2),
        BASE_DATE + timedelta(days=3),
        BASE_DATE + timedelta(days=4),
        BASE_DATE + timedelta(days=5),
        BASE_DATE + timedelta(days=6),
    ],
)
def test_is_weekend(input_date: datetime) -> Optional[bool]:
    assert Weekday.is_weekend(input_date) == (input_date.weekday() > 4)  # 4 is Friday


@pytest.mark.parametrize(
    "input_date, expected",
    [
        ("20231113", date(2023, 11, 13)),
        ("20231114", date(2023, 11, 14)),
        ("20231115", date(2023, 11, 15)),
        ("20231116", date(2023, 11, 16)),
        ("20231117", date(2023, 11, 17)),
        ("20231118", date(2023, 11, 18)),
        ("20231119", date(2023, 11, 19)),
    ],
)
def test_convert_to_date(input_date: str, expected: datetime) -> None:
    assert convert_to_date(input_date) == expected
