from datetime import date, datetime

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
    ],  # yapf: disable
)
def test_convert_to_date(input_date: str, expected: datetime) -> None:
    assert convert_to_date(input_date) == expected
