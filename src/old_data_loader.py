from datetime import datetime

from main import run

DATE_FORMAT = "%Y%m%d"


def save_data_local_until_now(private_key: str, start_date_str: str) -> None:
    end_date = datetime.now().date()

    run(
        start_date=start_date_str,
        end_date=end_date.strftime(DATE_FORMAT),
        private_key=private_key,
        result_type="xml",
        market_class="KOSDAQ",
        n_rows="1700",
        page_no="1",
        output_format="parquet",
        output_dir=start_date_str,
    )
