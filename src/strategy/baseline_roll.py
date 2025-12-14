"""
Baseline roll strategy - fixed-rule contract rolling.
"""
from datetime import date
from typing import Dict, Optional, Literal
from loguru import logger

from ..domain.contract import FuturesContract
from ..domain.chain import ContractChain
from ..data.signal_snapshot import SignalSnapshot
from ..account.account import Account
from .base import Strategy


class BaselineRollStrategy(Strategy):
    """
    Baseline index enhancement strategy with fixed rolling rules.
    
    Core idea:
    - Hold long futures position to capture index returns + basis convergence
    - Roll to next contract N days before expiry
    - Target leverage based on account equity
    """
    
    def __init__(
        self,
        contract_chain: ContractChain,
        roll_days_before_expiry: int = 2,
        contract_selection: Literal['nearby', 'next_nearby', 'volume', 'oi'] = 'nearby',
        target_leverage: float = 1.0,
        min_roll_days: int = 5,  # Minimum days to expiry for new contract
        signal_price_field: str = "open",  # Price field for signal calculation
    ):
        """
        Args:
            contract_chain: Contract chain to trade
            roll_days_before_expiry: Roll when days to expiry <= this value
            contract_selection: How to select new contract after roll
                - 'nearby': Nearest expiry contract
                - 'next_nearby': Second nearest expiry
                - 'volume': Highest volume
                - 'oi': Highest open interest
            target_leverage: Target notional / equity ratio
            min_roll_days: Minimum days to expiry for new contract selection
            signal_price_field: Price field for calculating target volume
        """
        super().__init__(contract_chain, signal_price_field)
        self.roll_days_before_expiry = roll_days_before_expiry
        self.contract_selection = contract_selection
        self.target_leverage = target_leverage
        self.min_roll_days = min_roll_days
        
        self._current_contract: Optional[FuturesContract] = None
    
    def on_bar(
        self,
        snapshot: SignalSnapshot,
        account: Account
    ) -> Dict[str, int]:
        """
        Generate target positions.
        
        Logic:
        1. If no position, select initial contract and open
        2. If holding, check if need to roll
        3. Calculate target volume based on equity and leverage
        """
        trade_date = snapshot.trade_date
        target_positions: Dict[str, int] = {}
        
        # Get current holding
        holding_contracts = account.get_holding_contracts()
        
        if not holding_contracts:
            # No position - select initial contract
            contract = self._select_contract(trade_date)
            if contract is None:
                logger.warning(f"No tradable contract on {trade_date}")
                return {}
            
            self._current_contract = contract
            volume = self._calculate_target_volume(contract, snapshot, account)
            target_positions[contract.ts_code] = volume
            
        else:
            # Have position - check if need to roll
            current_ts_code = holding_contracts[0]
            current_contract = self.contract_chain.get_contract(current_ts_code)
            
            if current_contract is None:
                logger.warning(f"Current contract not found: {current_ts_code}")
                return {}
            
            if self._should_roll(current_contract, snapshot):
                # Roll to new contract
                new_contract = self._select_roll_target(trade_date, current_contract)
                if new_contract is None:
                    logger.warning(f"No roll target found on {trade_date}")
                    # Keep current position
                    volume = self._calculate_target_volume(current_contract, snapshot, account)
                    target_positions[current_ts_code] = volume
                else:
                    # Close old, open new
                    target_positions[current_ts_code] = 0  # Close old
                    volume = self._calculate_target_volume(new_contract, snapshot, account)
                    target_positions[new_contract.ts_code] = volume
                    self._current_contract = new_contract
                    logger.info(f"Rolling {current_ts_code} -> {new_contract.ts_code} on {trade_date}")
            else:
                # No roll needed - maintain position
                volume = self._calculate_target_volume(current_contract, snapshot, account)
                target_positions[current_ts_code] = volume
                self._current_contract = current_contract
        
        return target_positions
    
    def _should_roll(self, contract: FuturesContract, snapshot: SignalSnapshot) -> bool:
        """Check if current contract should be rolled."""
        trade_date = snapshot.trade_date
        days_to_expiry = contract.days_to_expiry(trade_date)
        return days_to_expiry <= self.roll_days_before_expiry
    
    def _select_contract(self, trade_date: date) -> Optional[FuturesContract]:
        """Select initial contract based on selection rule."""
        if self.contract_selection == 'nearby':
            return self.contract_chain.get_main_contract(trade_date, rule='nearby')
        elif self.contract_selection == 'next_nearby':
            contracts = self.contract_chain.get_nearby_contracts(trade_date, k=2)
            return contracts[1] if len(contracts) > 1 else (contracts[0] if contracts else None)
        elif self.contract_selection == 'volume':
            return self.contract_chain.get_main_contract(trade_date, rule='volume')
        elif self.contract_selection == 'oi':
            return self.contract_chain.get_main_contract(trade_date, rule='oi')
        else:
            return self.contract_chain.get_main_contract(trade_date, rule='nearby')
    
    def _select_roll_target(
        self,
        trade_date: date,
        current_contract: FuturesContract
    ) -> Optional[FuturesContract]:
        """
        Select target contract for rolling.
        Excludes the current contract and contracts expiring too soon.
        """
        candidates = self.contract_chain.get_contracts_expiring_after(
            trade_date,
            min_days=self.min_roll_days
        )
        
        # Exclude current contract
        candidates = [c for c in candidates if c.ts_code != current_contract.ts_code]
        
        if not candidates:
            return None
        
        if self.contract_selection == 'nearby':
            return candidates[0]  # Already sorted by expiry
        elif self.contract_selection == 'next_nearby':
            return candidates[1]  # For roll, take nearest among valid
        elif self.contract_selection == 'volume':
            return max(candidates, key=lambda c: c.get_volume(trade_date))
        elif self.contract_selection == 'oi':
            return max(candidates, key=lambda c: c.get_open_interest(trade_date))
        else:
            return candidates[0]
    
    def _calculate_target_volume(
        self,
        contract: FuturesContract,
        snapshot: SignalSnapshot,
        account: Account
    ) -> int:
        """
        Calculate target volume based on equity and target leverage.
        
        Volume = (equity * leverage) / (price * multiplier)
        """
        price = snapshot.get_futures_price(contract.ts_code, self.signal_price_field)
        if price is None or price <= 0:
            return 0
        
        target_notional = account.equity * self.target_leverage
        contract_value = price * contract.multiplier
        
        volume = int(target_notional / contract_value)
        
        return max(volume, 0)  # Long only
    
    @property
    def current_contract(self) -> Optional[FuturesContract]:
        """Get the current holding contract."""
        return self._current_contract
