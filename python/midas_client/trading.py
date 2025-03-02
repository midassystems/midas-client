import requests
from typing import Dict
from .utils import load_url
from mbinary import BacktestData, LiveData, PyBacktestEncoder
import json


class TradingClient:
    def __init__(self, api_url: str = ""):
        if not api_url:
            api_url = load_url("MIDAS_URL")

        self.api_url = f"{api_url}/trading"
        # self.api_key = api_key

    def create_live(self, data: LiveData):
        url = f"{self.api_url}/live/create"

        response = requests.post(url, json=data.to_dict())

        if response.status_code != 200:
            raise ValueError(f"Create live failed: {response.text}")
        return response.json()

    def delete_live(self, id: int) -> Dict:
        url = f"{self.api_url}/live/delete"

        response = requests.delete(url, json=id)

        if response.status_code != 200:
            raise ValueError(f"Deleting live failed: {response.text}")
        return response.json()

    def get_live(self, id: int) -> Dict:
        url = f"{self.api_url}/live/get?id={id}"

        response = requests.get(url)

        if response.status_code != 200:
            raise ValueError(
                f"Live instance retrieval failed: {response.text}"
            )
        return response.json()

    def create_backtest(self, data: BacktestData) -> Dict:
        url = f"{self.api_url}/backtest/create"

        encoder = PyBacktestEncoder()
        buffer = encoder.encode_backtest(data)

        response = requests.post(url, data=buffer, stream=True)

        if response.status_code != 200:
            raise ValueError(f"Error while creating records : {response.text}")

        last_response = {}

        # Read the streamed content in chunks
        for chunk in response.iter_content(chunk_size=None):
            if chunk:
                try:
                    chunk_data = json.loads(chunk.decode("utf-8"))
                    last_response = chunk_data  # Keep updating with the latest parsed response
                except json.JSONDecodeError as e:
                    print(f"Failed to parse chunk as JSON: {e}")

        # Return the last response
        return last_response

    def delete_backtest(self, id: int) -> Dict:
        url = f"{self.api_url}/backtest/delete"

        response = requests.delete(url, json=id)

        if response.status_code != 200:
            raise ValueError(
                f"Instrument list retrieval failed: {response.text}"
            )
        return response.json()

    def get_backtest(self, id: int) -> Dict:
        url = f"{self.api_url}/backtest/get?id={id}"

        response = requests.get(url)

        if response.status_code != 200:
            raise ValueError(
                f"Instrument list retrieval failed: {response.text}"
            )
        return response.json()

    def get_backtest_by_name(self, name: str) -> Dict:
        url = f"{self.api_url}/backtest/get?name={name}"

        response = requests.get(url)

        if response.status_code != 200:
            raise ValueError(
                f"Instrument list retrieval failed: {response.text}"
            )
        return response.json()
