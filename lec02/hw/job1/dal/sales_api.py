import requests
from typing import List, Dict, Any

API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'


def get_sales(date: str) -> List[List[Dict[str, Any]]]:
    """
    Fetch paginated sales data from the API for the given date.

    :param date: The date for which sales data should be retrieved.
    :return: List of pages with sales records.
    """
    all_pages = []
    page = 1

    while True:
        try:
            # Fetch one page of data
            response = requests.get(f"{API_URL}sales", params={"date": date, "page": page})
            response.raise_for_status()
            page_data = response.json()

            if not page_data:
                break  # Stop if no data in this page

            all_pages.append(page_data)
            page += 1

        except requests.RequestException as e:
            print(f"Error fetching page {page} of sales data: {e}")
            break

    return all_pages
