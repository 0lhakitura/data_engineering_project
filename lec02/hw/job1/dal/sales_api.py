import requests

from lec02.hw.job1.main import AUTH_TOKEN

API_URL = "https://fake-api-vycpfa6oca-uc.a.run.app/"

def get_sales(date: str, page: int = 1):
    try:
        response = requests.get(
            f"{API_URL}sales",
            params={
                "date": date,
                "page": page
            },
            headers={
                "Authorization": f"Bearer {AUTH_TOKEN}"
            }
        )
        if response.status_code == 200:
            return response.json()
        return []
    except Exception as e:
        return []
