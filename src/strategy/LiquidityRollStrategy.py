"""
Smart roll strategy driven by liquidity crossover.
"""
from datetime import date
from typing import Optional, Literal
from loguru import logger

from ..domain.contract import FuturesContract
from ..domain.chain import ContractChain
from ..data.signal_snapshot import SignalSnapshot
from .baseline_roll import BaselineRollStrategy


class LiquidityRollStrategy(BaselineRollStrategy):
    """
    Liquidity-based smart roll strategy.

    Logic:
    1. Monitor the roll target determined by `contract_selection`.
    2. Trigger a roll when the target contract's liquidity
       (Volume or Open Interest) exceeds that of the current holding.
    3. Safety rule: force a roll when the contract is within
       `roll_days_before_expiry` days of expiration.
    """

    def __init__(
        self,
        contract_chain: ContractChain,
        roll_days_before_expiry: int = 1,     # Forced roll threshold (days)
        contract_selection: str = "nearby",  # Roll target selection rule
        target_leverage: float = 1.0,
        min_roll_days: int = 5,
        signal_price_field: str = "open",
        roll_criteria: Literal["volume", "oi"] = "volume",  # Liquidity metric
        **kwargs,
    ):
        super().__init__(
            contract_chain=contract_chain,
            roll_days_before_expiry=roll_days_before_expiry,
            contract_selection=contract_selection,
            target_leverage=target_leverage,
            min_roll_days=min_roll_days,
            signal_price_field=signal_price_field,
        )
        self.roll_criteria = roll_criteria
        # The legacy _check_next_contract helper is no longer required.

    def _should_roll(
        self,
        contract: FuturesContract,
        snapshot: SignalSnapshot,
    ) -> bool:
        """
        Determine whether a roll should be executed based on
        liquidity crossover or forced expiry conditions.

        Note:
        The roll target is identified via the base class
        `_select_roll_target` method, and its liquidity is
        compared against the current holding.
        """
        trade_date = snapshot.trade_date

        # 1. Safety check: force roll close to expiration
        days_to_expiry = contract.days_to_expiry(trade_date)
        if days_to_expiry <= self.roll_days_before_expiry:
            return True

        # 2. Identify roll candidate using base selection logic
        candidate = self._select_roll_target(trade_date, contract)
        if candidate is None:
            # No eligible roll target available
            return False

        # 3. Liquidity comparison using T-1 data
        current_val = 0.0
        candidate_val = 0.0

        if self.roll_criteria == "volume":
            current_val = snapshot.get_prev_volume(contract.ts_code) or 0.0
            candidate_val = snapshot.get_prev_volume(candidate.ts_code) or 0.0
        elif self.roll_criteria == "oi":
            current_val = snapshot.get_prev_oi(contract.ts_code) or 0.0
            candidate_val = snapshot.get_prev_oi(candidate.ts_code) or 0.0

        # Trigger roll if candidate is more liquid
        if candidate_val > current_val and candidate_val > 0:
            return True