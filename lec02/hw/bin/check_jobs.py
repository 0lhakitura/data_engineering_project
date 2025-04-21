import os
import time

import requests
from dotenv import load_dotenv
load_dotenv()


BASE_DIR = os.environ.get("BASE_DIR")
if not BASE_DIR:
    print("Error: BASE_DIR environment variable is not set or is empty.")
    exit(1)
else:
    print(f"BASE_DIR is set to: {BASE_DIR}")


JOB1_PORT = 8081
JOB2_PORT = 8082

RAW_DIR = os.path.join(BASE_DIR, "raw", "sales", "2022-08-09")
STG_DIR = os.path.join(BASE_DIR, "stg", "sales", "2022-08-09")


def run_job1():
    url = f'http://localhost:{JOB1_PORT}/'

    auth_token = os.environ.get("AUTH_TOKEN")
    if not auth_token:
        raise Exception("AUTH_TOKEN environment variable must be set.")

    # Prepare the JSON payload for the request
    payload = {
        "date": "2022-08-09",
        "base_dir": BASE_DIR
    }

    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }

    try:
        resp = requests.post(url, json=payload, headers=headers)
        print(f"Status code: {resp.status_code}")
        print(f"Response body: {resp.text}")

        assert resp.status_code == 201, "Expected 201 Created response!"
        print("Job1 ran successfully!")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while running Job1: {e}")
    except AssertionError:
        print(f"Unexpected response: {resp.text}")
"""
 def run_job1():
    print("Starting job1:")
    resp = requests.post(
        url=f'http://localhost:{JOB1_PORT}/',
        json={
            "date": "2022-08-09",
            "raw_dir": RAW_DIR
        }
    )
    print(f"Status code: {resp.status_code}")
    print(f"Response body: {resp.text}")
    print(RAW_DIR)
    assert resp.status_code == 201
    print("job1 completed!")
 """
def run_job2():
    print("Starting job2:")
    resp = requests.post(
        url=f'http://localhost:{JOB2_PORT}/',
        json={
            "raw_dir": RAW_DIR,
            "stg_dir": STG_DIR
        }
    )
    assert resp.status_code == 201
    print("job2 completed!")


if __name__ == '__main__':
    run_job1()
    time.sleep(3)
    run_job2()
