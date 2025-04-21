"""
Tests for main.py
"""
import os
from unittest import TestCase, mock

# NB: avoid relative imports when you will write your code
from .. import main
from ..main import AUTH_TOKEN


class MainFunctionTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()

    @mock.patch('lec02.hw.job1.main.save_sales_to_local_disk')
    def test_return_400_date_param_missed(
        self,
        get_sales_mock: mock.MagicMock
    ):
        """
        Raise 400 HTTP code when no 'date' param
        """
        resp = self.client.post(
            '/',
            json={
                'raw_dir': './foo/bar/',
                # no 'date' set!
            },
        )
        self.assertEqual(400, resp.status_code)

    @mock.patch('lec02.hw.job1.main.save_sales_to_local_disk')
    def test_return_400_raw_dir_param_missed(
        self,
        get_sales_mock: mock.MagicMock
    ):
        """
        Raise 400 HTTP code when no 'raw_dir' param
        """
        resp = self.client.post(
            '/',
            json={
                'date': '1970-01-01',
                # no 'raw_dir' set!
            },
        )
        self.assertEqual(400, resp.status_code)

    @mock.patch('lec02.hw.job1.main.save_sales_to_local_disk')
    def test_save_sales_to_local_disk(
            self,
            save_sales_to_local_disk_mock: mock.MagicMock
    ):
        """
        Test whether save_sales_to_local_disk is called with proper params
        """
        fake_date = '2022-08-09'
        fake_base_dir = './foo/bar'
        expected_raw_dir = os.path.join(fake_base_dir, "raw", "sales", fake_date)

        response = self.client.post(
            '/',
            json={
                'date': fake_date,
                'base_dir': fake_base_dir,
            },
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        self.assertEqual(201, response.status_code)
        save_sales_to_local_disk_mock.assert_called_with(
            date=fake_date,
            raw_dir=expected_raw_dir,
        )

    @mock.patch('lec02.hw.job1.main.save_sales_to_local_disk')
    def test_return_201_when_all_is_ok(
            self,
            get_sales_mock: mock.MagicMock
    ):
        fake_date = '1970-01-01'
        fake_base_dir = './foo/bar/'

        resp = self.client.post(
            '/',
            json={
                'date': fake_date,
                'base_dir': fake_base_dir,
            },
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        self.assertEqual(201, resp.status_code)
