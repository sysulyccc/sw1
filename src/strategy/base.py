"""
Abstract strategy base class.
"""
from abc import ABC, abstractmethod
from typing import Dict, Union

from ..domain.chain import ContractChain
from ..data.snapshot import MarketSnapshot
from ..data.signal_snapshot import SignalSnapshot
from ..account.account import Account


class Strategy(ABC):
    """
    Abstract base class for trading strategies.
    
    A strategy takes market snapshot and account state as input,
    and outputs target positions (contract -> volume).
    """
    
    def __init__(
        self,
        contract_chain: ContractChain,
        signal_price_field: str = "open",
    ):
        """
        Args:
            contract_chain: The contract chain to trade
            signal_price_field: Price field for signal calculation (open, pre_settle, close)
        """
        self.contract_chain = contract_chain
        self.signal_price_field = signal_price_field
    
    @abstractmethod
    def on_bar(
        self,
        snapshot: SignalSnapshot,
        account: Account
    ) -> Dict[str, int]:
        """
        Generate target positions based on current market and account state.
        
        IMPORTANT: The snapshot is a RESTRICTED SignalSnapshot that only provides
        data available at signal time (T-day open, pre_settle, T-1 day data).
        You CANNOT access T-day close, settle, volume, etc.
        
        Args:
            snapshot: Restricted signal snapshot (prevents lookahead bias)
            account: Current account state
            
        Returns:
            Dict mapping contract code (ts_code) to target volume.
            Positive volume = long, negative = short, 0 = no position.
        """
        pass
    
    @property
    def fut_code(self) -> str:
        """Get the futures code this strategy trades."""
        return self.contract_chain.fut_code
    
    @property
    def index(self):
        """Get the underlying index."""
        return self.contract_chain.index
