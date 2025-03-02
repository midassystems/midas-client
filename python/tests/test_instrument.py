import os
import unittest
from mbinary import Dataset, Vendors
import requests
from dotenv import load_dotenv
from midas_client import DatabaseClient

# Load url
load_dotenv()

MIDAS_URL = os.getenv("MIDAS_URL")

if MIDAS_URL is None:
    raise ValueError("MIDAS_URL environment variable is not set")


# Helper methods
def create_instruments(ticker: str, name: str) -> int:
    url = f"{MIDAS_URL}/instruments/create"
    data = {
        "ticker": ticker,
        "name": name,
        "dataset": "Equities",
        "vendor": "Databento",
        "vendor_data": 12303838,
        "last_available": 234565432,
        "first_available": 234546762,
        "expiration_date": 234565432,
        "active": True,
    }

    response = requests.post(url, json=data).json()

    id = response["data"]
    return id


def delete_instruments(id: int) -> None:
    url = f"{MIDAS_URL}/instruments/delete"

    _ = requests.delete(url, json=id)


class TestClientMethods(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = DatabaseClient()

    def test_get_instrument(self):
        ticker = "APELR"
        name = "testing"
        id = create_instruments(ticker, name)

        # Test
        response = self.client.instrument.get_instrument(
            ticker,
            Dataset.EQUITIES,
        )

        # Validate
        self.assertEqual(response["code"], 200)
        self.assertEqual(len(response["data"]), 1)

        # Cleanup
        delete_instruments(id)

    def test_dataset_instrument(self):
        ticker = "APELR"
        name = "testing"
        id = create_instruments(ticker, name)

        # Test
        response = self.client.instrument.list_dataset_instruments(
            Dataset.EQUITIES
        )

        # Validate
        self.assertEqual(response["code"], 200)
        self.assertEqual(len(response["data"]), 1)

        # Cleanup
        delete_instruments(id)

    def test_vendor_instrument(self):
        ticker = "APELR"
        name = "testing"
        id = create_instruments(ticker, name)

        # Test
        response = self.client.instrument.list_vendor_instruments(
            Vendors.DATABENTO, Dataset.EQUITIES
        )

        # Validate
        self.assertEqual(response["code"], 200)
        self.assertEqual(len(response["data"]), 1)

        # Cleanup
        delete_instruments(id)


if __name__ == "__main__":
    unittest.main()
