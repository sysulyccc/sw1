
import pytest
from datetime import date
from unittest.mock import MagicMock, PropertyMock

from src.backtest.engine import BacktestEngine
from src.account.account import Account
from src.data.handler import DataHandler
from src.data.signal_snapshot import SignalSnapshot
from src.data.snapshot import MarketSnapshot
from src.domain.contract import FuturesContract
from src.domain.bars import FuturesDailyBar, IndexDailyBar
from src.backtest.nav_tracker import FixedLotNormalizedNavTracker

class TestTradingFlow:
    """
    Tests for critical trading flow logic:
    1. Dynamic Margin Update
    2. Intraday Timeline (Signal -> Trade -> Settle)
    """

    @pytest.fixture
    def mock_data_handler(self):
        handler = MagicMock(spec=DataHandler)
        
        # Setup ContractChain mock explicitly
        chain_mock = MagicMock()
        contract = FuturesContract(
            ts_code="IC2401.CFX",
            fut_code="IC",
            multiplier=200.0,
            list_date=date(2023, 1, 1),
            delist_date=date(2024, 1, 19)
        )
        # Mock add_bar method to avoid errors if called
        contract.add_bar = MagicMock()
        contract.get_price = MagicMock(return_value=5000.0)
        
        chain_mock.contracts = {"IC2401.CFX": contract}
        handler.contract_chain = chain_mock
        
        return handler

    @pytest.fixture
    def engine(self, mock_data_handler):
        # Create engine with dynamic margin enabled
        engine = BacktestEngine(
            data_handler=mock_data_handler,
            strategy=MagicMock(),
            initial_capital=1_000_000.0,
            margin_rate=0.12,
            use_dynamic_margin=True
        )
        # Initialize account manually as run() does
        engine.account = Account(
            initial_capital=1_000_000.0,
            margin_rate=0.12,
            # use_dynamic_margin=True  <-- Removed
        )
        return engine

    def test_dynamic_margin_update(self, engine, mock_data_handler):
        """Test that margin rate updates daily based on DataHandler."""
        trade_date = date(2015, 9, 7)
        
        # Setup mock return for margin rate
        # 2015-09-07 was a high margin day (e.g. 40%)
        mock_data_handler.get_margin_rate.return_value = 0.40
        
        # Mock snapshots to allow _process_day to run without error
        signal_snap = MagicMock(spec=SignalSnapshot)
        signal_snap.trade_date = trade_date
        mock_data_handler.get_signal_snapshot.return_value = signal_snap
        
        market_snap = MagicMock(spec=MarketSnapshot)
        market_snap.trade_date = trade_date
        mock_data_handler.get_snapshot.return_value = market_snap
        
        # Run one day processing
        # We need to ensure contract_chain is accessible
        engine._process_day(trade_date, mock_data_handler.contract_chain.contracts)
        
        # Verify DataHandler was queried
        mock_data_handler.get_margin_rate.assert_called_with(trade_date, default=0.12)
        
        # Verify Account margin rate was updated
        assert engine.account.margin_rate == 0.40

    def test_intraday_timeline_correctness(self, engine, mock_data_handler):
        """
        Verify the timeline:
        09:30: SignalSnapshot (Open Price) -> Trade Execution
        15:00: MarketSnapshot (Settle Price) -> Mark to Market
        """
        trade_date = date(2024, 1, 5)
        ts_code = "IC2401.CFX"
        
        # 1. Setup Signal Snapshot (Morning)
        # Price is OPEN price (5000.0)
        signal_snap = MagicMock(spec=SignalSnapshot)
        signal_snap.trade_date = trade_date
        signal_snap.get_futures_price.return_value = 5000.0
        mock_data_handler.get_signal_snapshot.return_value = signal_snap
        
        # 2. Setup Market Snapshot (End of Day)
        # Settle price is 5050.0 (Rise of 50 points)
        market_snap = MagicMock(spec=MarketSnapshot)
        market_snap.trade_date = trade_date
        
        # Mock bar in snapshot
        mock_bar = MagicMock(spec=FuturesDailyBar)
        mock_bar.settle = 5050.0
        market_snap.futures_quotes = {ts_code: mock_bar}
        
        # Setup contract price for mark_to_market
        contract = mock_data_handler.contract_chain.contracts[ts_code]
        contract.get_price = MagicMock(return_value=5050.0)
        
        mock_data_handler.get_snapshot.return_value = market_snap
        
        # 3. Strategy Logic: Buy 1 lot
        engine.strategy.on_bar.return_value = {ts_code: 1}
        
        # --- Run the Day ---
        engine._process_day(trade_date, mock_data_handler.contract_chain.contracts)
        
        # --- Verify Execution (Morning) ---
        # Should have executed at 5000.0 (Open)
        pos = engine.account.get_position(ts_code)
        assert pos is not None
        assert pos.entry_price == 5000.0
        assert pos.volume == 1
        
        # Verify execution queried the signal snapshot price
        signal_snap.get_futures_price.assert_called_with(ts_code, "open")
        
        # --- Verify Settlement (Evening) ---
        # Account cash should reflect daily PnL
        # PnL = (Settle - Entry) * Vol * Multiplier
        # PnL = (5050 - 5000) * 1 * 200 = 10,000
        # Commission = 5000 * 1 * 200 * 0.00023 = 230
        
        expected_pnl = (5050.0 - 5000.0) * 1 * 200.0
        expected_commission = 5000.0 * 1 * 200.0 * 0.00023
        
        # Check if cash includes PnL - Commission
        # Initial cash 1,000,000
        expected_cash = 1_000_000.0 - expected_commission + expected_pnl
        
        assert abs(engine.account.cash - expected_cash) < 1e-6
        
        # Position last_settle should be updated to today's settle
        assert pos.last_settle == 5050.0

    def test_fixed_lot_normalized_nav(self, engine, mock_data_handler):
        trade_date = date(2024, 1, 5)
        ts_code = "IC2401.CFX"

        engine.strategy.position_mode = "fixed_lot"

        signal_snap = MagicMock(spec=SignalSnapshot)
        signal_snap.trade_date = trade_date
        signal_snap.get_futures_price.return_value = 5000.0
        mock_data_handler.get_signal_snapshot.return_value = signal_snap

        market_snap = MagicMock(spec=MarketSnapshot)
        market_snap.trade_date = trade_date

        mock_bar = MagicMock(spec=FuturesDailyBar)
        mock_bar.settle = 5050.0
        market_snap.futures_quotes = {ts_code: mock_bar}

        contract = mock_data_handler.contract_chain.contracts[ts_code]
        contract.get_price = MagicMock(return_value=5050.0)
        mock_data_handler.get_snapshot.return_value = market_snap

        engine.strategy.on_bar.return_value = {ts_code: 1}

        engine._process_day(trade_date, mock_data_handler.contract_chain.contracts)

        base = 5000.0 * 1 * 200.0
        commission = base * 0.00023
        daily_pnl = (5050.0 - 5000.0) * 1 * 200.0
        expected_nav = (base - commission + daily_pnl) / base

        assert isinstance(engine._nav_tracker, FixedLotNormalizedNavTracker)
        assert engine._nav_tracker.notional_base == base
        assert trade_date in engine._nav_tracker.nav_history
        assert abs(engine._nav_tracker.nav_history[trade_date] - expected_nav) < 1e-12

    def test_roll_logic_execution(self, engine, mock_data_handler):
        """
        Verify rolling logic:
        Close old contract -> Open new contract
        Check commissions and PnL continuity.
        """
        trade_date = date(2024, 1, 17) # Roll day
        old_code = "IC2401.CFX"
        new_code = "IC2402.CFX"
        
        # Setup contracts
        old_contract = mock_data_handler.contract_chain.contracts[old_code]
        new_contract = FuturesContract(
            ts_code=new_code,
            fut_code="IC",
            multiplier=200.0,
            list_date=date(2023, 2, 1),
            delist_date=date(2024, 2, 16)
        )
        mock_data_handler.contract_chain.contracts[new_code] = new_contract
        
        # Setup Account: Holding 1 lot of OLD contract
        # Assume entered at 4900, yesterday settle was 4950
        pos = engine.account._execute_trade(
            old_contract, 
            volume=1, 
            price=4900.0, 
            trade_date=date(2024, 1, 16), 
            reason="INIT"
        )
        engine.account.positions[old_code].last_settle = 4950.0
        engine.account.cash = 1_000_000.0 # Reset cash for clarity

        engine.strategy.position_mode = "fixed_lot"
        engine._nav_tracker = FixedLotNormalizedNavTracker()
        engine._nav_tracker.reset()
        engine._nav_tracker._notional_base = 4900.0 * 1 * 200.0
        engine._nav_tracker._equity = engine._nav_tracker._notional_base
        
        # Setup Signal Snapshot (Morning)
        # Old Open: 5000 (Gap up from 4950)
        # New Open: 5100
        signal_snap = MagicMock(spec=SignalSnapshot)
        signal_snap.trade_date = trade_date
        
        def get_price_side_effect(code, field):
            if code == old_code: return 5000.0
            if code == new_code: return 5100.0
            return None
        signal_snap.get_futures_price.side_effect = get_price_side_effect
        mock_data_handler.get_signal_snapshot.return_value = signal_snap
        
        # Setup Market Snapshot (Evening)
        # Old Settle: 5020
        # New Settle: 5120
        market_snap = MagicMock(spec=MarketSnapshot)
        market_snap.trade_date = trade_date
        
        old_bar = MagicMock(spec=FuturesDailyBar); old_bar.settle = 5020.0
        new_bar = MagicMock(spec=FuturesDailyBar); new_bar.settle = 5120.0
        market_snap.futures_quotes = {old_code: old_bar, new_code: new_bar}
        
        mock_data_handler.get_snapshot.return_value = market_snap
        
        # Configure contracts get_price for mark_to_market
        old_contract.get_price = MagicMock(return_value=5020.0)
        new_contract.get_price = MagicMock(return_value=5120.0)

        # Strategy Decision: Roll!
        # Sell 1 Old, Buy 1 New
        engine.strategy.on_bar.return_value = {old_code: 0, new_code: 1}
        
        # --- Run the Day ---
        engine._process_day(trade_date, mock_data_handler.contract_chain.contracts)
        
        # --- Verify Positions ---
        assert old_code not in engine.account.positions
        assert new_code in engine.account.positions
        assert engine.account.positions[new_code].volume == 1
        assert engine.account.positions[new_code].entry_price == 5100.0 # Open price
        
        # --- Verify Cash Flows ---
        # 1. Close Old:
        # Realized PnL (from Entry 4900 to Close 5000) is NOT fully cash settled immediately in this system
        # WAIT! execute_trade calculates Realized PnL but update_volume updates last_settle
        # Actually in this system:
        # Commission = 5000 * 200 * 0.00023 = 230
        
        # 2. Open New:
        # Commission = 5100 * 200 * 0.00023 = 234.6
        
        # 3. Mark to Market (Evening):
        # The new position will generate PnL from 5100 (Entry) to 5120 (Settle)
        # PnL_new = (5120 - 5100) * 1 * 200 = 4000
        
        # 4. Total Expected Cash Change:
        # - Commission_Close_Old: 5000 * 200 * 0.00023 = 230
        # + PnL_Close_Old (Settlement): (5000 - 4950) * 1 * 200 = 10000  <-- This is likely missing in current impl
        # - Commission_Open_New: 5100 * 200 * 0.00023 = 234.6
        # + PnL_Hold_New: (5120 - 5100) * 1 * 200 = 4000
        
        commission_old = 5000 * 1 * 200 * 0.00023
        pnl_settle_old = (5000 - 4950) * 1 * 200
        
        commission_new = 5100 * 1 * 200 * 0.00023
        pnl_hold_new = (5120 - 5100) * 1 * 200
        
        # Initial cash was reset to 1,000,000
        expected_cash = 1_000_000.0 - commission_old + pnl_settle_old - commission_new + pnl_hold_new
        
        # Print for debugging
        print(f"\nExpected Cash: {expected_cash}")
        print(f"Actual Cash:   {engine.account.cash}")
        
        # This assertion verifies if the PnL from closing the old position was added to cash
        assert abs(engine.account.cash - expected_cash) < 1e-6

        base = 4900.0 * 1 * 200.0
        pnl_settle_old_to_open = (5000.0 - 4950.0) * 1 * 200.0
        commission_old_norm = 5000.0 * 1 * 200.0 * 0.00023
        commission_new_norm = 5100.0 * 1 * 200.0 * 0.00023
        pnl_hold_new_norm = (5120.0 - 5100.0) * 1 * 200.0
        expected_norm_nav = (base + pnl_settle_old_to_open - commission_old_norm - commission_new_norm + pnl_hold_new_norm) / base

        assert isinstance(engine._nav_tracker, FixedLotNormalizedNavTracker)
        assert trade_date in engine._nav_tracker.nav_history
        assert abs(engine._nav_tracker.nav_history[trade_date] - expected_norm_nav) < 1e-12
 

