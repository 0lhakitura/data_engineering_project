import unittest
import tempfile
import os
import json
from fastavro import reader
from flask.testing import FlaskClient
from lec02.hw.job2.main import app

class TestSaveSalesToAvro(unittest.TestCase):

    def setUp(self):
        self.client: FlaskClient = app.test_client()

        # temp dirs
        self.raw_dir = tempfile.mkdtemp()
        self.stg_dir = tempfile.mkdtemp()

        # sample JSON file
        self.sample_data = {
            "client": "Michael Wilkerson",
            "purchase_date": "2022-08-09",
            "product": "Vacuum cleaner",
            "price": 346.0
        }
        self.json_path = os.path.join(self.raw_dir, "sale1.json")
        with open(self.json_path, "w") as f:
            json.dump(self.sample_data, f)

    def tearDown(self):
        # clean up temp dirs
        for d in [self.raw_dir, self.stg_dir]:
            for f in os.listdir(d):
                os.remove(os.path.join(d, f))
            os.rmdir(d)

    def test_missing_parameters(self):
        resp = self.client.post("/", json={})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Missing raw_dir or stg_dir", resp.get_json()["message"])

    def test_invalid_raw_dir(self):
        resp = self.client.post("/", json={
            "raw_dir": "/nonexistent/path",
            "stg_dir": self.stg_dir
        })
        self.assertEqual(resp.status_code, 400)
        self.assertIn("does not exist", resp.get_json()["message"])

    def test_no_json_files(self):
        # empty raw dir
        empty_dir = tempfile.mkdtemp()
        resp = self.client.post("/", json={
            "raw_dir": empty_dir,
            "stg_dir": self.stg_dir
        })
        self.assertEqual(resp.status_code, 400)
        self.assertIn("No JSON files found", resp.get_json()["message"])
        os.rmdir(empty_dir)

    def test_successful_conversion(self):
        resp = self.client.post("/", json={
            "raw_dir": self.raw_dir,
            "stg_dir": self.stg_dir
        })
        self.assertEqual(resp.status_code, 201)
        data = resp.get_json()
        self.assertIn("Successfully converted", data["message"])

        avro_path = data["output_path"]
        self.assertTrue(os.path.exists(avro_path))

        # Check Avro content
        with open(avro_path, "rb") as f:
            records = list(reader(f))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], self.sample_data)

if __name__ == "__main__":
    unittest.main()

