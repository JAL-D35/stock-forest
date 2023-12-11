from main import run

DATE_FORMAT = "%Y%m%d"


def save_data_date_range(
    private_key: str, start_date_str: str, end_date_str: str, output_dir: str
) -> None:
    run(
        start_date=start_date_str,
        end_date=end_date_str,
        private_key=private_key,
        result_type="xml",
        market_class="KOSDAQ",
        n_rows="1700",
        page_no="1",
        output_format="parquet",
        output_dir=output_dir,
    )
