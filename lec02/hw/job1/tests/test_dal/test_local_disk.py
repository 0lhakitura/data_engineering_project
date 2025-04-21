"""
Tests dal.local_disk.py module
# TODO: write tests
"""
import json
import os
from unittest import TestCase, mock

from lec02.hw.job1.dal.local_disk import save_to_disk


class SaveToDiskTestCase(TestCase):
    """
    Test dal.local_disk.save_to_disk function.
    # TODO: implement
    """
    def setUp(self):
        """
        Set up resources for the tests. Runs before every test.
        """
        self.test_dir = "test_storage"  # Temporary directory for testing
        self.test_file = os.path.join(self.test_dir, "test_file.json")
        self.test_content = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]

        # Ensure the test directory exists
        os.makedirs(self.test_dir, exist_ok=True)

    def tearDown(self):
        """
        Clean up after tests. Runs after every test.
        """
        # Remove test directory and its contents
        if os.path.exists(self.test_dir):
            for file in os.listdir(self.test_dir):
                os.remove(os.path.join(self.test_dir, file))
            os.rmdir(self.test_dir)

    def test_save_to_disk_creates_file(self):
        """
        Test that the save_to_disk function correctly creates a file.
        """
        save_to_disk(self.test_content, self.test_file)

        # Assert that the file now exists in the test directory
        self.assertTrue(os.path.exists(self.test_file))

    def test_save_to_disk_file_content(self):
        """
        Test that the written JSON file contains the correct content.
        """
        save_to_disk(self.test_content, self.test_file)

        # Read the file and verify the content
        with open(self.test_file, "r", encoding="utf-8") as f:
            saved_content = json.load(f)

        # Assert the content matches
        self.assertEqual(saved_content, self.test_content)

    def test_save_to_disk_overwrites_file(self):
        """
        Test that save_to_disk overwrites the file if it already exists.
        """
        # Save initial content
        initial_content = [{"name": "Charlie", "age": 20}]
        save_to_disk(initial_content, self.test_file)

        # Save new content
        save_to_disk(self.test_content, self.test_file)

        with open(self.test_file, "r", encoding="utf-8") as f:
            saved_content = json.load(f)

        # Assert the file now contains the new content
        self.assertEqual(saved_content, self.test_content)

    def test_save_to_disk_invalid_path(self):
        """
        Test that save_to_disk raises an exception for invalid file paths.
        """
        # Provide a directory path instead of a file path
        invalid_path = self.test_dir

        with self.assertRaises(IsADirectoryError):
            save_to_disk(self.test_content, invalid_path)
