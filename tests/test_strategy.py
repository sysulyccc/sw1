"""
Tests for strategy layer (Layer 4).
"""
import pytest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

from src.data.handler import DataHandler
from src.account.account import Account
from src.strategy.baseline_roll import BaselineRollStrategy
from src.strategy.basis_timing import BasisTimingStrategy
from src.data.signal_snapshot import SignalSnapshot


class TestBaselineRollStrategy:
    """Tests for BaselineRollStrategy."""
    
    @pytest.fixture
    def data_handler(self):
        data_path = Path("/root/sw1/processed_data")
        if not data_path.exists():
            pytest.skip("Processed data not available")
        return DataHandler.from_processed_data(str(data_path), "IC")
    
    @pytest.fixture
    def strategy(self, data_handler):
        return BaselineRollStrategy(
            contract_chain=data_handler.contract_chain,
            roll_days_before_expiry=2,
            contract_selection='nearby',
            target_leverage=1.0,
        )
    
    @pytest.fixture
    def account(self):
        return Account(initial_capital=10_000_000.0)
    
    def test_initial_position(self, strategy, data_handler, account):
        calendar = data_handler.get_trading_calendar()
        trade_date = calendar[100]  # Skip initial period
        
        snapshot = data_handler.get_signal_snapshot(trade_date)
        target = strategy.on_bar(snapshot, account)
        
        # Should have one position
        assert len(target) == 1
        # Should be positive volume (long)
        assert list(target.values())[0] > 0
    
    def test_roll_detection(self, strategy, data_handler, account):
        # Find a contract near expiry
        chain = data_handler.contract_chain
        
        # Manually test roll logic
        contracts = chain.get_all_contracts()
        for contract in contracts[:5]:
            # Get a date close to expiry
            delist = contract.delist_date
            test_date = date(delist.year, delist.month, delist.day - 2)
            
            if chain.get_active_contracts(test_date):
                snapshot = MagicMock(spec=SignalSnapshot)
                snapshot.trade_date = test_date
                should_roll = strategy._should_roll(contract, snapshot)
                assert should_roll is True


class TestBasisTimingStrategy:
    """Tests for BasisTimingStrategy."""
    
    @pytest.fixture
    def data_handler(self):
        data_path = Path("/root/sw1/processed_data")
        if not data_path.exists():
            pytest.skip("Processed data not available")
        return DataHandler.from_processed_data(str(data_path), "IC")
    
    @pytest.fixture
    def strategy(self, data_handler):
        return BasisTimingStrategy(
            contract_chain=data_handler.contract_chain,
            roll_days_before_expiry=2,
            basis_entry_threshold=-0.02,
            basis_exit_threshold=0.005,
        )
    
    def test_timing_signal(self, strategy):
        # Test absolute threshold mode
        strategy.use_percentile = False
        
        # Deep discount -> ENTER
        assert strategy._get_timing_signal(-0.03) == "ENTER"
        
        # Premium -> EXIT
        assert strategy._get_timing_signal(0.01) == "EXIT"
        
        # In between -> HOLD
        assert strategy._get_timing_signal(-0.01) == "HOLD"


def test_basis_timing_roll_day_uses_new_contract(monkeypatch):
    base_targets = {"IC1907.CFX": 0, "IC1908.CFX": 10}

    def _fake_baseline_on_bar(self, snapshot, account):
        return base_targets

    monkeypatch.setattr(BaselineRollStrategy, "on_bar", _fake_baseline_on_bar, raising=True)

    strategy = BasisTimingStrategy(contract_chain=MagicMock())
    snapshot = MagicMock(spec=SignalSnapshot)
    snapshot.trade_date = date(2019, 7, 16)

    def _fake_get_basis(ts_code: str, relative: bool = True, use_prev_close: bool = False):
        if ts_code == "IC1908.CFX":
            return -0.03
        return -0.01

    snapshot.get_basis.side_effect = _fake_get_basis

    account = MagicMock(spec=Account)
    target = strategy.on_bar(snapshot, account)

    assert target.get("IC1907.CFX") == 0
    assert target.get("IC1908.CFX") == 10


def test_basis_timing_uses_prev_close_flag(monkeypatch):
    base_targets = {"IC1908.CFX": 10}

    def _fake_baseline_on_bar(self, snapshot, account):
        return base_targets

    monkeypatch.setattr(BaselineRollStrategy, "on_bar", _fake_baseline_on_bar, raising=True)

    strategy = BasisTimingStrategy(contract_chain=MagicMock(), basis_use_prev_close=True)
    snapshot = MagicMock(spec=SignalSnapshot)
    snapshot.trade_date = date(2019, 7, 16)
    snapshot.get_basis.return_value = -0.03

    account = MagicMock(spec=Account)
    strategy.on_bar(snapshot, account)

    assert snapshot.get_basis.call_count >= 1
    _, kwargs = snapshot.get_basis.call_args
    assert kwargs.get("use_prev_close") is True


def test_basis_timing_neutral_hold_baseline(monkeypatch):
    base_targets = {"IC1908.CFX": 10}

    def _fake_baseline_on_bar(self, snapshot, account):
        return base_targets

    monkeypatch.setattr(BaselineRollStrategy, "on_bar", _fake_baseline_on_bar, raising=True)

    strategy = BasisTimingStrategy(contract_chain=MagicMock(), neutral_hold_baseline=True)
    snapshot = MagicMock(spec=SignalSnapshot)
    snapshot.trade_date = date(2019, 7, 16)
    snapshot.get_basis.return_value = -0.01

    account = MagicMock(spec=Account)
    target = strategy.on_bar(snapshot, account)

    assert target.get("IC1908.CFX") == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
