from datetime import datetime, timedelta

import pytest

from .holiday_checker import Weekday, convert_to_date

BASE_DATETIME = datetime(2024, 1, 1)  # 2024/01/01 is Monday


@pytest.mark.parametrize(
    "input_date, expected",
    [
        (BASE_DATETIME, False),
        (BASE_DATETIME + timedelta(days=1), False),
        (BASE_DATETIME + timedelta(days=2), False),
        (BASE_DATETIME + timedelta(days=3), False),
        (BASE_DATETIME + timedelta(days=4), False),
        (BASE_DATETIME + timedelta(days=5), True),
        (BASE_DATETIME + timedelta(days=6), True),
    ],
)
def test_is_weekend(input_date: datetime, expected: bool) -> None:
    assert expected == Weekday.is_weekend(input_date)


def test_convert_to_date() -> None:
    input_date = BASE_DATETIME.strftime("%Y%m%d")  # e.g. 20210101
    expected = BASE_DATETIME.date()
    assert convert_to_date(input_date) == expected
