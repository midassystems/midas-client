from dotenv import load_dotenv
from .historical import HistoricalClient
from .trading import TradingClient
from .instrument import InstrumentClient


class DatabaseClient:
    def __init__(self):
        load_dotenv()

        self.historical = HistoricalClient()
        self.trading = TradingClient()
        self.instrument = InstrumentClient()
        # self.api_key = api_key
