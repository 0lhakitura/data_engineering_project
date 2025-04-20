import os
import requests
from dotenv import load_dotenv
load_dotenv()

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
                "Authorization": f"Bearer {os.environ.get('AUTH_TOKEN')}"
            }
        )
        if response.status_code == 200:
            return response.json()
        return []
    except Exception as e:
        return []
