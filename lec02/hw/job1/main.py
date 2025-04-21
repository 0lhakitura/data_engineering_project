"""
This file contains the controller that accepts commands via HTTP
and triggers the business logic layer.
"""
import os
from flask import Flask, request, jsonify
from flask import typing as flask_typing
from lec02.hw.job1.bll.sales_api import save_sales_to_local_disk


AUTH_TOKEN = os.environ.get("AUTH_TOKEN")

if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")

app = Flask(__name__)


@app.route("/", methods=["POST"])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts commands via HTTP and triggers the business logic layer.

    Proposed POST body in JSON:
    {
      "date": "2022-08-09",
      "base_dir": "./cat"
    }
    """
    # Parse JSON input
    input_data: dict = request.json

    # Check for missing input
    if not input_data:
        return jsonify({"message": "Missing request body"}), 400

    # Extract parameters
    date = input_data.get("date")
    base_dir = input_data.get("base_dir")

    # Validate `date`
    if not date:
        return jsonify({"message": "Parameter 'date' is missing"}), 400

    # Validate `base_dir`
    if not base_dir:
        return jsonify({"message": "Parameter 'base_dir' is missing"}), 400

    # Optional: Authentication via Authorization header
    auth_header = request.headers.get("Authorization")
    if not auth_header or auth_header != f"Bearer {AUTH_TOKEN}":
        return jsonify({"message": "Unauthorized"}), 401

    # Compute the directory path where raw data should be stored
    raw_dir = os.path.join(base_dir, "raw", "sales", date)

    try:
        # Trigger business logic to save sales data
        save_sales_to_local_disk(date=date, raw_dir=raw_dir)

        return jsonify({"message": f"Sales data for date {date} saved successfully."}), 201

    except Exception as e:
        # Handle unexpected errors
        print(f"Error: {e}")
        return jsonify({"message": "An error occurred while processing your request.", "error": str(e)}), 500


if __name__ == "__main__":
    # Run the Flask app
    app.run(debug=True, host="localhost", port=8081)