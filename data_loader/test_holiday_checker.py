from datetime import datetime

import pytest

from .holiday_checker import Weekday, convert_to_date

BASE_DATETIME = datetime(2023, 11, 13)


@pytest.mark.parametrize(
    "input_date, expected",
    [
        (datetime(2023, 11, 13), False),
        (datetime(2023, 11, 14), False),
        (datetime(2023, 11, 15), False),
        (datetime(2023, 11, 16), False),
        (datetime(2023, 11, 17), False),
        (datetime(2023, 11, 18), True),
        (datetime(2023, 11, 19), True),
    ],
)
def test_is_weekend(input_date: datetime, expected: bool) -> None:
    assert Weekday.is_weekend(input_date) == expected
