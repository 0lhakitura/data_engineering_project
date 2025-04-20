"""
Tests for main.py
# TODO: write tests
"""
import os
import tempfile
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
        resp = self.client.get(
            '/',
            json={
                'raw_dir': '/foo/bar/',
                # no 'date' set!
            },
        )

        self.assertEqual(400, resp.status_code)


    @mock.patch('lec02.hw.job1.main.save_sales_to_local_disk')
    def test_return_400_raw_dir_param_missed(
            self,
            save_sales_to_local_disk_mock: mock.MagicMock
        ):
        """
        Raise 400 HTTP code when no 'raw_dir' param
        """
        resp = self.client.get(
            '/',
            json={
                'date': '1970-01-01',
                # no 'raw_dir' set!
            },
        )
        self.assertEqual(400, resp.status_code)

    @mock.patch('lec02.hw.job1.bll.sales_api.save_sales_to_local_disk')
    def test_save_sales_to_local_disk(self, save_sales_to_local_disk_mock):
        fake_date = '2022-08-09'
        with tempfile.TemporaryDirectory() as tmpdirname:
            fake_base_dir = tmpdirname

            resp = self.client.get(
                '/',
                json={
                    'date': fake_date,
                    'base_dir': fake_base_dir,
                },
                headers={
                    'Authorization': f'Bearer {AUTH_TOKEN}'
                }
            )

            expected_raw_dir = os.path.join(fake_base_dir, "raw", "sales", fake_date)

            save_sales_to_local_disk_mock.assert_called_with(
                date=fake_date,
                raw_dir=expected_raw_dir,
            )

    @mock.patch('lec02.hw.job1.main.save_sales_to_local_disk')
    def test_return_201_when_all_is_ok(
            self,
            get_sales_mock: mock.MagicMock
    ):
        """
        Return 201 HTTP code when params are valid and get_sales works
        """

        # Підставляємо фейкову відповідь API
        get_sales_mock.return_value = [{"id": 1, "amount": 100}]

        fake_date = '1970-01-01'
        fake_raw_dir = '/foo/bar/'

        resp = self.client.get(
            '/',
            json={
                'date': fake_date,
                'raw_dir': fake_raw_dir,
            },
        )

        # Перевіряємо що повернуло 201
        self.assertEqual(201, resp.status_code)

        # І що викликався наш мок
        get_sales_mock.assert_called_with(fake_date)
