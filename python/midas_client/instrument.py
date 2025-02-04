import requests
from .utils import load_url
from mbinary import Dataset, Vendors


class InstrumentClient:
    def __init__(self, api_url: str = ""):
        if not api_url:
            api_url = load_url("INSTRUMENT_URL")

        self.api_url = f"{api_url}/instruments"

    def get_instrument(self, ticker: str, dataset: Dataset):
        url = f"{self.api_url}/get"
        payload = (ticker, dataset.to_json())
        response = requests.get(url, json=payload)

        if response.status_code != 200:
            raise ValueError(
                f"Instrument list retrieval failed: {response.text}"
            )
        return response.json()

    def list_dataset_instruments(self, dataset: Dataset):
        url = f"{self.api_url}/list_dataset"
        response = requests.get(url, json=dataset.to_json())

        if response.status_code != 200:
            raise ValueError(
                f"Instrument list retrieval failed: {response.text}"
            )
        return response.json()

    def list_vendor_instruments(self, vendor: Vendors, dataset: Dataset):
        url = f"{self.api_url}/list_vendor"
        payload = (vendor.to_json(), dataset.to_json())
        response = requests.get(url, json=payload)

        if response.status_code != 200:
            raise ValueError(
                f"Instrument list retrieval failed: {response.text}"
            )
        return response.json()
