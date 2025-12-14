
import pytest
from datetime import date
from unittest.mock import MagicMock

from src.strategy.smart_roll import SmartRollStrategy
from src.domain.contract import FuturesContract
from src.domain.chain import ContractChain
from src.data.signal_snapshot import SignalSnapshot

class TestSmartRollStrategy:
    
    @pytest.fixture
    def strategy(self):
        # Mock ContractChain
        chain = MagicMock(spec=ContractChain)
        
        # Setup contracts
        c1 = FuturesContract("IC2401.CFX", "IC", 200, date(2023,1,1), date(2024,1,19))
        c2 = FuturesContract("IC2402.CFX", "IC", 200, date(2023,2,1), date(2024,2,16))
        
        chain.get_contracts_expiring_after.return_value = [c2]

        chain.trading_days_to_expiry.return_value = 10
        
        # Init strategy
        strat = SmartRollStrategy(
            contract_chain=chain,
            roll_days_before_expiry=1, # Safety net
            min_roll_days=5,
            roll_criteria='volume'
        )
        # Attach contracts to strategy for easy access in tests
        strat.c1 = c1
        strat.c2 = c2
        return strat

    def test_should_roll_volume_crossover(self, strategy):
        """Test rolling when candidate volume > current volume."""
        trade_date = date(2024, 1, 10) # 9 days to expiry for c1 (safe)
        
        # Mock snapshot
        snapshot = MagicMock(spec=SignalSnapshot)
        snapshot.trade_date = trade_date
        snapshot.get_basis.side_effect = lambda code, relative=True: -0.02 if code == "IC2401.CFX" else -0.03
        strategy.contract_chain.trading_days_to_expiry.return_value = 10
        
        # Case 1: Current Volume > Candidate Volume -> NO ROLL
        snapshot.get_prev_volume.side_effect = lambda code: 10000 if code == "IC2401.CFX" else 5000
        
        should_roll = strategy._should_roll(strategy.c1, snapshot)
        assert should_roll is False
        
        # Case 2: Candidate Volume > Current Volume -> ROLL
        snapshot.get_prev_volume.side_effect = lambda code: 8000 if code == "IC2401.CFX" else 9000
        
        should_roll = strategy._should_roll(strategy.c1, snapshot)
        assert should_roll is True

    def test_should_roll_force_expiry(self, strategy):
        """Test forced rolling when expiry is imminent regardless of liquidity."""
        # 1 day to expiry for c1 (Jan 19) -> Jan 18
        trade_date = date(2024, 1, 18) 
        
        snapshot = MagicMock(spec=SignalSnapshot)
        snapshot.trade_date = trade_date

        strategy.contract_chain.trading_days_to_expiry.return_value = 0
        snapshot.get_basis.side_effect = lambda code, relative=True: -0.02 if code == "IC2401.CFX" else -0.03
        
        # Even if Candidate Volume < Current Volume
        snapshot.get_prev_volume.side_effect = lambda code: 10000 if code == "IC2401.CFX" else 100
        
        should_roll = strategy._should_roll(strategy.c1, snapshot)
        assert should_roll is True

    def test_should_roll_oi_crossover(self, strategy):
        """Test rolling based on Open Interest."""
        strategy.roll_criteria = 'oi'
        trade_date = date(2024, 1, 10)
        
        snapshot = MagicMock(spec=SignalSnapshot)
        snapshot.trade_date = trade_date
        snapshot.get_basis.side_effect = lambda code, relative=True: -0.02 if code == "IC2401.CFX" else -0.03
        
        # Case 1: Current OI > Candidate OI -> NO ROLL
        snapshot.get_prev_oi.side_effect = lambda code: 50000 if code == "IC2401.CFX" else 10000
        assert strategy._should_roll(strategy.c1, snapshot) is False
        
        # Case 2: Candidate OI > Current OI -> ROLL
        snapshot.get_prev_oi.side_effect = lambda code: 40000 if code == "IC2401.CFX" else 45000
        assert strategy._should_roll(strategy.c1, snapshot) is True
