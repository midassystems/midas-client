import unittest
from midas_client.utils import iso_to_unix


class TestClientMethods(unittest.TestCase):
    def test_isodate_to_unix(self):
        date = "2024-09-01"

        # Test
        unix = iso_to_unix(date)
        self.assertEqual(1725148800000000000, unix)

    def test_isodatetime_to_unix(self):
        date = "2024-09-01T00:00:00Z"

        # Test
        unix = iso_to_unix(date)
        self.assertEqual(1725148800000000000, unix)

    def test_datetime_to_unix(self):
        date = "2024-09-01 00:00:00"

        # Test
        unix = iso_to_unix(date)
        self.assertEqual(1725148800000000000, unix)

    def test_datetime3_to_unix(self):
        date = "2024-09-01T00:00:00-04:00"

        # Test
        unix = iso_to_unix(date)
        self.assertEqual(1725163200000000000, unix)


if __name__ == "__main__":
    unittest.main()
