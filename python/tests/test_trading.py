import unittest
import os
import requests
from dotenv import load_dotenv
from midas_client import DatabaseClient
import json
import mbinary

# Load url
load_dotenv()

MIDAS_URL = os.getenv("MIDAS_URL")

if MIDAS_URL is None:
    raise ValueError("MIDAS_URL environment variable is not set")


# Helper methods
def create_instruments(ticker: str, name: str) -> int:
    url = f"{MIDAS_URL}/historical/instruments/create"
    data = {
        "ticker": ticker,
        "name": name,
        "vendor": "databento",
        "stype": "continuous",
        "dataset": "test",
        "last_available": 1,
        "first_available": 0,
        "is_continous": False,
        "active": True,
    }

    response = requests.post(url, json=data).json()

    id = response["data"]
    return id


def delete_instruments(id: int) -> None:
    url = f"{MIDAS_URL}/historical/instruments/delete"

    _ = requests.delete(url, json=id).json()


def create_records(binary_data: list):
    url = f"{MIDAS_URL}/historical/mbp/create"

    _ = requests.post(url, json=binary_data)


def create_backtest():
    with open("tests/data/test_data.backtest.json", "r") as f:
        data = json.load(f)

    # Parameters
    parameters = mbinary.Parameters(
        strategy_name=data["parameters"]["strategy_name"],
        capital=data["parameters"]["capital"],
        schema=data["parameters"]["schema"],
        data_type=data["parameters"]["data_type"],
        start=data["parameters"]["start"],
        end=data["parameters"]["end"],
        tickers=data["parameters"]["tickers"],
    )

    # Static Stats
    static_stats = mbinary.StaticStats(
        total_trades=data["static_stats"]["total_trades"],
        total_winning_trades=data["static_stats"]["total_winning_trades"],
        total_losing_trades=data["static_stats"]["total_losing_trades"],
        avg_profit=data["static_stats"]["avg_profit"],
        avg_profit_percent=data["static_stats"]["avg_profit_percent"],
        avg_gain=data["static_stats"]["avg_gain"],
        avg_gain_percent=data["static_stats"]["avg_gain_percent"],
        avg_loss=data["static_stats"]["avg_loss"],
        avg_loss_percent=data["static_stats"]["avg_loss_percent"],
        profitability_ratio=data["static_stats"]["profitability_ratio"],
        profit_factor=data["static_stats"]["profit_factor"],
        profit_and_loss_ratio=data["static_stats"]["profit_and_loss_ratio"],
        total_fees=data["static_stats"]["total_fees"],
        net_profit=data["static_stats"]["net_profit"],
        beginning_equity=data["static_stats"]["beginning_equity"],
        ending_equity=data["static_stats"]["ending_equity"],
        total_return=data["static_stats"]["total_return"],
        annualized_return=data["static_stats"]["annualized_return"],
        daily_standard_deviation_percentage=data["static_stats"][
            "daily_standard_deviation_percentage"
        ],
        annual_standard_deviation_percentage=data["static_stats"][
            "annual_standard_deviation_percentage"
        ],
        max_drawdown_percentage_period=data["static_stats"][
            "max_drawdown_percentage_period"
        ],
        max_drawdown_percentage_daily=data["static_stats"][
            "max_drawdown_percentage_daily"
        ],
        sharpe_ratio=data["static_stats"]["sharpe_ratio"],
        sortino_ratio=data["static_stats"]["sortino_ratio"],
    )

    # Period TimeseriesStats
    period_timeseries_stats = [
        mbinary.TimeseriesStats(
            timestamp=stat["timestamp"],
            equity_value=stat["equity_value"],
            percent_drawdown=stat["percent_drawdown"],
            cumulative_return=stat["cumulative_return"],
            period_return=stat["period_return"],
        )
        for stat in data["period_timeseries_stats"]
    ]

    # Daily TimeseriesStats
    daily_timeseries_stats = [
        mbinary.TimeseriesStats(
            timestamp=stat["timestamp"],
            equity_value=stat["equity_value"],
            percent_drawdown=stat["percent_drawdown"],
            cumulative_return=stat["cumulative_return"],
            period_return=stat["period_return"],
        )
        for stat in data["daily_timeseries_stats"]
    ]

    # Trades
    trades = [
        mbinary.Trades(
            trade_id=trade["trade_id"],
            signal_id=trade["signal_id"],
            timestamp=trade["timestamp"],
            ticker=trade["ticker"],
            quantity=trade["quantity"],
            avg_price=trade["avg_price"],
            trade_value=trade["trade_value"],
            trade_cost=trade["trade_cost"],
            action=trade["action"],
            fees=trade["fees"],
        )
        for trade in data["trades"]
    ]

    # Signals
    signals = []
    for signal_data in data["signals"]:
        trade_instructions = [
            mbinary.SignalInstructions(
                ticker=instr["ticker"],
                order_type=instr["order_type"],
                action=instr["action"],
                signal_id=instr["signal_id"],
                weight=instr["weight"],
                quantity=instr["quantity"],
                limit_price=instr.get("limit_price", ""),
                aux_price=instr.get("aux_price", ""),
            )
            for instr in signal_data["trade_instructions"]
        ]

        # Signal object with the list of SignalInstructions
        signal = mbinary.Signals(
            timestamp=signal_data["timestamp"],
            trade_instructions=trade_instructions,
        )
        signals.append(signal)

    # Construct and return the BacktestData object
    metadata = mbinary.BacktestMetaData(
        backtest_id=0,
        backtest_name=data["backtest_name"],
        parameters=parameters,
        static_stats=static_stats,
    )

    return mbinary.BacktestData(
        metadata=metadata,
        period_timeseries_stats=period_timeseries_stats,
        daily_timeseries_stats=daily_timeseries_stats,
        trades=trades,
        signals=signals,
    )


def create_live():
    with open("tests/data/test_data.live.json", "r") as f:
        data = json.load(f)

    # Parameters
    parameters = mbinary.Parameters(
        strategy_name=data["parameters"]["strategy_name"],
        capital=data["parameters"]["capital"],
        schema=data["parameters"]["schema"],
        data_type=data["parameters"]["data_type"],
        start=data["parameters"]["start"],
        end=data["parameters"]["end"],
        tickers=data["parameters"]["tickers"],
    )

    # Account Summary
    account = mbinary.AccountSummary(
        currency=data["account"]["currency"],
        start_buying_power=data["account"]["start_buying_power"],
        start_excess_liquidity=data["account"]["start_excess_liquidity"],
        start_full_available_funds=data["account"][
            "start_full_available_funds"
        ],
        start_full_init_margin_req=data["account"][
            "start_full_init_margin_req"
        ],
        start_full_maint_margin_req=data["account"][
            "start_full_maint_margin_req"
        ],
        start_futures_pnl=data["account"]["start_futures_pnl"],
        start_net_liquidation=data["account"]["start_net_liquidation"],
        start_total_cash_balance=data["account"]["start_total_cash_balance"],
        start_unrealized_pnl=data["account"]["start_unrealized_pnl"],
        start_timestamp=data["account"]["start_timestamp"],
        end_buying_power=data["account"]["end_buying_power"],
        end_excess_liquidity=data["account"]["end_excess_liquidity"],
        end_full_available_funds=data["account"]["end_full_available_funds"],
        end_full_init_margin_req=data["account"]["end_full_init_margin_req"],
        end_full_maint_margin_req=data["account"]["end_full_maint_margin_req"],
        end_futures_pnl=data["account"]["end_futures_pnl"],
        end_net_liquidation=data["account"]["end_net_liquidation"],
        end_total_cash_balance=data["account"]["end_total_cash_balance"],
        end_unrealized_pnl=data["account"]["end_unrealized_pnl"],
        end_timestamp=data["account"]["end_timestamp"],
    )

    # Trades
    trades = [
        mbinary.Trades(
            trade_id=trade["trade_id"],
            signal_id=trade["signal_id"],
            timestamp=trade["timestamp"],
            ticker=trade["ticker"],
            quantity=trade["quantity"],
            avg_price=trade["avg_price"],
            trade_value=trade["trade_value"],
            trade_cost=trade["trade_cost"],
            action=trade["action"],
            fees=trade["fees"],
        )
        for trade in data["trades"]
    ]

    # Signals
    signals = []
    for signal_data in data["signals"]:
        trade_instructions = [
            mbinary.SignalInstructions(
                ticker=instr["ticker"],
                order_type=instr["order_type"],
                action=instr["action"],
                signal_id=instr["signal_id"],
                weight=instr["weight"],
                quantity=instr["quantity"],
                limit_price=instr.get("limit_price", ""),
                aux_price=instr.get("aux_price", ""),
            )
            for instr in signal_data["trade_instructions"]
        ]

        signal = mbinary.Signals(
            timestamp=signal_data["timestamp"],
            trade_instructions=trade_instructions,
        )
        signals.append(signal)

    # Construct and return the BacktestData object
    return mbinary.LiveData(
        live_id=None,
        parameters=parameters,
        trades=trades,
        signals=signals,
        account=account,
    )


class TestClientMethods(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = DatabaseClient()

    # @unittest.skip("")
    def test_create_live(self):
        # Setup
        live = create_live()

        # Test
        response = self.client.trading.create_live(live)
        id = response["data"]

        # Validate
        self.assertEqual(response["code"], 200)

        # Cleanup
        self.client.trading.delete_live(id)

    # @unittest.skip("")
    def test_get_live(self):
        # # Setup
        live = create_live()
        response = self.client.trading.create_live(live)
        id = response["data"]

        # Test
        response = self.client.trading.get_live(id)

        # Validate
        self.assertEqual(response["code"], 200)

        # Cleanup
        self.client.trading.delete_live(id)

    # @unittest.skip("")
    def test_create_backtest(self):
        # Setup
        backtest = create_backtest()

        # Test
        response = self.client.trading.create_backtest(backtest)
        id = int(response["data"])

        # Validate
        self.assertEqual(response["code"], 200)

        # Cleanup
        self.client.trading.delete_backtest(id)

    # @unittest.skip("")
    def test_get_backtest(self):
        # # Setup
        backtest = create_backtest()
        response = self.client.trading.create_backtest(backtest)
        id = int(response["data"])

        # Test
        response = self.client.trading.get_backtest(id)

        # Validate
        self.assertEqual(response["code"], 200)

        # Cleanup
        self.client.trading.delete_backtest(id)

    # @unittest.skip("")
    def test_get_backtest_by_name(self):
        # # Setup
        backtest = create_backtest()
        response = self.client.trading.create_backtest(backtest)
        id = int(response["data"])

        # Test
        response = self.client.trading.get_backtest_by_name("testing76543")

        # Validate
        self.assertEqual(response["code"], 200)

        # Cleanup
        self.client.trading.delete_backtest(id)


if __name__ == "__main__":
    unittest.main()
