"""
Tests for domain layer (Layer 1).
"""
import pytest
from datetime import date

from src.domain.bars import IndexDailyBar, FuturesDailyBar
from src.domain.index import EquityIndex
from src.domain.contract import FuturesContract
from src.domain.chain import ContractChain


class TestIndexDailyBar:
    """Tests for IndexDailyBar."""
    
    def test_create_bar(self):
        bar = IndexDailyBar(
            trade_date=date(2024, 1, 1),
            open=5000.0,
            high=5100.0,
            low=4900.0,
            close=5050.0,
        )
        assert bar.trade_date == date(2024, 1, 1)
        assert bar.close == 5050.0
    
    def test_bar_immutable(self):
        bar = IndexDailyBar(
            trade_date=date(2024, 1, 1),
            open=5000.0,
            high=5100.0,
            low=4900.0,
            close=5050.0,
        )
        with pytest.raises(Exception):  # dataclass(frozen=True)
            bar.close = 5100.0


class TestFuturesDailyBar:
    """Tests for FuturesDailyBar."""
    
    def test_create_bar(self):
        bar = FuturesDailyBar(
            trade_date=date(2024, 1, 1),
            open=5000.0,
            high=5100.0,
            low=4900.0,
            close=5050.0,
            settle=5040.0,
            pre_settle=5000.0,
            volume=10000.0,
            amount=500000.0,
            open_interest=50000.0,
        )
        assert bar.settle == 5040.0
        assert bar.volume == 10000.0


class TestEquityIndex:
    """Tests for EquityIndex."""
    
    @pytest.fixture
    def sample_index(self):
        index = EquityIndex("000905.SH", "CSI500")
        for i in range(5):
            bar = IndexDailyBar(
                trade_date=date(2024, 1, i + 1),
                open=5000.0 + i * 10,
                high=5100.0 + i * 10,
                low=4900.0 + i * 10,
                close=5050.0 + i * 10,
            )
            index.add_bar(bar)
        return index
    
    def test_add_and_get_bar(self, sample_index):
        bar = sample_index.get_bar(date(2024, 1, 1))
        assert bar is not None
        assert bar.close == 5050.0
    
    def test_get_close(self, sample_index):
        close = sample_index.get_close(date(2024, 1, 3))
        assert close == 5070.0
    
    def test_get_trading_dates(self, sample_index):
        dates = sample_index.get_trading_dates()
        assert len(dates) == 5
        assert dates[0] == date(2024, 1, 1)


class TestFuturesContract:
    """Tests for FuturesContract."""
    
    @pytest.fixture
    def sample_contract(self):
        contract = FuturesContract(
            ts_code="IC2401.CFX",
            fut_code="IC",
            multiplier=200.0,
            list_date=date(2023, 10, 1),
            delist_date=date(2024, 1, 19),
        )
        # Add some bars
        for i in range(5):
            bar = FuturesDailyBar(
                trade_date=date(2024, 1, i + 1),
                open=5000.0 + i * 10,
                high=5100.0 + i * 10,
                low=4900.0 + i * 10,
                close=5050.0 + i * 10,
                settle=5040.0 + i * 10,
                pre_settle=5000.0 + i * 10,
                volume=10000.0 - i * 500,
                amount=500000.0,
                open_interest=50000.0,
            )
            contract.add_bar(bar)
        return contract
    
    def test_is_tradable(self, sample_contract):
        # Before list date
        assert not sample_contract.is_tradable(date(2023, 9, 1))
        # During trading period
        assert sample_contract.is_tradable(date(2024, 1, 5))
        # After delist date
        assert not sample_contract.is_tradable(date(2024, 1, 20))
    
    def test_days_to_expiry(self, sample_contract):
        days = sample_contract.days_to_expiry(date(2024, 1, 10))
        assert days == 9  # Jan 19 - Jan 10
    
    def test_get_price(self, sample_contract):
        settle = sample_contract.get_price(date(2024, 1, 3), 'settle')
        assert settle == 5060.0
    
    def test_get_volume(self, sample_contract):
        volume = sample_contract.get_volume(date(2024, 1, 1))
        assert volume == 10000.0


class TestContractChain:
    """Tests for ContractChain."""
    
    @pytest.fixture
    def sample_chain(self):
        index = EquityIndex("000905.SH", "CSI500")
        chain = ContractChain(index, "IC")
        
        # Add two contracts
        for month, delist in [(1, 19), (2, 16)]:
            contract = FuturesContract(
                ts_code=f"IC240{month}.CFX",
                fut_code="IC",
                multiplier=200.0,
                list_date=date(2023, 10, 1),
                delist_date=date(2024, month, delist),
            )
            # Add bars
            for i in range(5):
                bar = FuturesDailyBar(
                    trade_date=date(2024, 1, i + 1),
                    open=5000.0,
                    high=5100.0,
                    low=4900.0,
                    close=5050.0,
                    settle=5040.0,
                    pre_settle=5000.0,
                    volume=10000.0 if month == 1 else 5000.0,
                    amount=500000.0,
                    open_interest=50000.0 if month == 1 else 30000.0,
                )
                contract.add_bar(bar)
            chain.add_contract(contract)
        
        return chain
    
    def test_get_active_contracts(self, sample_chain):
        active = sample_chain.get_active_contracts(date(2024, 1, 5))
        assert len(active) == 2
    
    def test_get_main_contract_by_volume(self, sample_chain):
        main = sample_chain.get_main_contract(date(2024, 1, 5), rule='volume')
        assert main.ts_code == "IC2401.CFX"  # Higher volume
    
    def test_get_nearby_contracts(self, sample_chain):
        nearby = sample_chain.get_nearby_contracts(date(2024, 1, 5), k=2)
        assert len(nearby) == 2
        assert nearby[0].ts_code == "IC2401.CFX"  # Nearest expiry

    def test_trading_days_to_expiry_anchored_at_last_tradable_day(self, sample_chain):
        calendar = [
            date(2024, 1, 15),
            date(2024, 1, 16),
            date(2024, 1, 17),
            date(2024, 1, 18),
            date(2024, 1, 19),
        ]
        sample_chain.set_trading_calendar(calendar)

        contract = sample_chain.get_contract("IC2401.CFX")
        assert contract is not None

        assert sample_chain.trading_days_to_expiry(contract, date(2024, 1, 15)) == 3
        assert sample_chain.trading_days_to_expiry(contract, date(2024, 1, 16)) == 2
        assert sample_chain.trading_days_to_expiry(contract, date(2024, 1, 17)) == 1
        assert sample_chain.trading_days_to_expiry(contract, date(2024, 1, 18)) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
