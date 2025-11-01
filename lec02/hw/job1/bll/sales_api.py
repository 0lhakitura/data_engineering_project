import os
import shutil
from lec02.hw.job1.dal.local_disk import save_to_disk
from lec02.hw.job1.dal.sales_api import get_sales


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    """
    Fetch sales data from the API for the given date and save it to the local disk.

    :param date: Date for which the sales data should be fetched (format: YYYY-MM-DD).
    :param raw_dir: Directory path where the sales data should be stored.
    :return: None
    """
    # Ensure the target directory exists and is empty
    if os.path.exists(raw_dir):
        # Remove all contents of the directory (clear previous data)
        shutil.rmtree(raw_dir)
    os.makedirs(raw_dir, exist_ok=True)

    # Fetch paginated sales data from the API
    all_sales_data = get_sales(date)

    # Save each page of data in a separate JSON file or in one file
    if isinstance(all_sales_data, list) and len(all_sales_data) > 0:
        for i, page in enumerate(all_sales_data, start=1):
            # Save each page into a separate JSON file if paginated
            file_name = f"sales_{date}_{i}.json" if len(all_sales_data) > 1 else f"sales_{date}.json"
            file_path = os.path.join(raw_dir, file_name)
            save_to_disk(page, file_path)

    print(f"Sales data for {date} saved to {raw_dir}.")
