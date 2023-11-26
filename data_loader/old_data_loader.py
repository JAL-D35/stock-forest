from datetime import date, datetime, timedelta
from typing import Generator, List, Optional

from .holiday_checker import convert_to_date
from .main import run

DATE_FORMAT = "%Y%m%d"


def calculate_delta(start_date: date, end_date: date) -> Optional[int]:
    if end_date > start_date:
        return (end_date - start_date).days


def get_target_date(start_date: date, delta: int) -> Generator[str, None, None]:
    for i in range(delta + 1):
        yield (start_date + timedelta(days=i)).strftime(DATE_FORMAT)


def load_old_data(private_key: str, start_date_str: str) -> None:
    start_date = convert_to_date(start_date_str)
    end_date = datetime.now().date()
    delta = calculate_delta(start_date, end_date)

    if delta is None:
        return

    for target_date_str in get_target_date(start_date, delta):
        run(
            input_date=target_date_str,
            private_key=private_key,
            result_type="xml",
            market_class="KOSDAQ",
            n_rows="1690",
            page_no="1",
            output_format="parquet",
            output_dir=target_date_str,
        )
