import requests
from typing import List
from mbinary import BufferStore, RetrieveParams
from .utils import load_url
import json


class HistoricalClient:
    def __init__(self, api_url: str = ""):
        if not api_url:
            api_url = load_url("HISTORICAL_URL")

        self.api_url = f"{api_url}/historical"
        # self.api_key = api_key

    def create_records(self, data: List[int]):
        """
        Stream loading main used for testing.
        """

        url = f"{self.api_url}/mbp/create/stream"

        response = requests.post(url, json=data, stream=True)

        if response.status_code != 200:
            raise ValueError(f"Error while creating records : {response.text}")

        last_response = None

        # Read the streamed content in chunks
        for chunk in response.iter_content(chunk_size=None):
            if chunk:  # filter out keep-alive chunks
                try:
                    chunk_data = json.loads(chunk.decode("utf-8"))
                    last_response = chunk_data  # Keep updating with the latest parsed response
                except json.JSONDecodeError as e:
                    print(f"Failed to parse chunk as JSON: {e}")

        # Return the last response
        return last_response

    def get_records(self, params: RetrieveParams):
        url = f"{self.api_url}/mbp/get/stream"

        # Deserialize JSON string into a Python dictionary
        payload_dict = json.loads(params.to_json())
        response = requests.get(url, json=payload_dict, stream=True)

        if response.status_code != 200:
            raise ValueError(
                f"Instrument list retrieval failed: {response.text}"
            )

        bin_data = bytearray()

        # Read the streamed content in chunks
        for chunk in response.iter_content(chunk_size=None):
            if chunk:  # filter out keep-alive or non-binary chunks
                if b"Finished streaming all batches" in chunk:
                    continue
                bin_data.extend(chunk)

        return BufferStore(bytes(bin_data))
