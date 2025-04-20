import unittest
from unittest.mock import patch, MagicMock

from lec02.hw.job1.dal.sales_api import get_sales
from dotenv import load_dotenv

from lec02.hw.job1.main import AUTH_TOKEN

load_dotenv()

class GetSalesTestCase(unittest.TestCase):
    """
    Test sales_api.get_sales function.
    """

    @patch("lec02.hw.job1.dal.sales_api.requests.get")
    def test_get_sales_success(self, mock_get):
        """
        Test that get_sales successfully fetches sales data from the API.
        """
        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"client": "Alice", "purchase_date": "2022-08-09", "product": "Laptop", "price": 1200},
            {"client": "Bob", "purchase_date": "2022-08-09", "product": "Tablet", "price": 800},
        ]
        mock_get.return_value = mock_response

        # Call the function
        date = "2022-08-09"
        result = get_sales(date)

        # Assert the result matches the mocked API data
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["client"], "Alice")
        self.assertEqual(result[1]["product"], "Tablet")

        # Ensure the correct API endpoint is called
        mock_get.assert_called_once_with(
            "https://fake-api-vycpfa6oca-uc.a.run.app/sales",
            params={"date": date, "page": 1},
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"}
        )

    @patch("lec02.hw.job1.dal.sales_api.requests.get")
    def test_get_sales_404_error(self, mock_get):
        """
        Test that get_sales raises an exception or returns an empty list when the API returns a 404.
        """
        # Mock API response for 404
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        # Call the function and handle the error case
        date = "2022-08-09"
        result = get_sales(date)

        # Assert that result is empty when 404 is encountered
        self.assertEqual(result, [])

    @patch("lec02.hw.job1.dal.sales_api.requests.get")
    def test_get_sales_connection_error(self, mock_get):
        """
        Test that get_sales appropriately handles a connection error.
        """
        # Mock a connection error
        mock_get.side_effect = Exception("Connection error")

        # Call the function and handle the error case
        date = "2022-08-09"
        result = get_sales(date)

        # Assert that result is empty on connection error
        self.assertEqual(result, [])  # Modify based on actual behavior of get_sales

    @patch("lec02.hw.job1.dal.sales_api.requests.get")
    def test_get_sales_empty_response(self, mock_get):
        """
        Test that get_sales handles an empty response without errors.
        """
        # Mock API response for empty data
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        # Call the function
        date = "2022-08-09"
        result = get_sales(date)

        # Assert that the result is an empty list
        self.assertEqual(result, [])
