from datetime import date
from typing import Dict, Optional
from loguru import logger
import numpy as np
from ..config import TRADING_DAYS_PER_YEAR
from ..domain.contract import FuturesContract
from ..domain.chain import ContractChain
from ..data.signal_snapshot import SignalSnapshot
from ..account.account import Account
from .baseline_roll import BaselineRollStrategy


class AERYRollStrategy(BaselineRollStrategy):
    """
    Optimal maturity selection strategy with a fixed roll trigger.

    Objective:
    - Compute the contract with the highest Annualized Expected Roll Yield (AERY) daily.
    - Execute a roll only when the current contract approaches expiry,
      as defined by a fixed number of days before expiration.
    """

    def __init__(
        self,
        contract_chain: ContractChain,
        roll_days_before_expiry: int = 2,  # Fixed roll trigger in days
        target_leverage: float = 1.0,
        min_roll_days: int = 5,
        signal_price_field: str = "open",
        **kwargs
    ):
        # Enable BaselineRollStrategy._should_roll via roll_days_before_expiry
        super().__init__(
            contract_chain,
            roll_days_before_expiry=roll_days_before_expiry,
            contract_selection="optimal_aery",  # Internal identifier
            target_leverage=target_leverage,
            min_roll_days=min_roll_days,
            signal_price_field=signal_price_field,
        )

        # Cache for the daily optimal roll target
        self._optimal_roll_target: Optional[FuturesContract] = None

    def _calculate_annualized_roll_yield(
        self,
        contract: FuturesContract,
        snapshot: SignalSnapshot,
        price_field: str,
    ) -> Optional[float]:
        """
        Compute the Annualized Expected Roll Yield (AERY) for a given contract.

        The calculation logic is consistent with the original
        OptimalMaturityStrategy.
        """
        futures_price = snapshot.get_futures_price(contract.ts_code, price_field)
        index_price = snapshot.get_index_price(price_field)
        trade_date = snapshot.trade_date

        if futures_price is None or index_price is None or futures_price == 0:
            return None

        days_to_expiry = contract.days_to_expiry(trade_date)
        if days_to_expiry <= 0:
            return None

        # Roll profit ratio: (Spot - Futures) / Futures
        roll_profit_ratio = (index_price - futures_price) / futures_price

        # AERY = roll profit ratio annualized by remaining days
        aery = roll_profit_ratio * (TRADING_DAYS_PER_YEAR / days_to_expiry)

        return aery

    def _select_optimal_target(
        self,
        trade_date: date,
        snapshot: SignalSnapshot,
    ) -> Optional[FuturesContract]:
        """
        Helper method: select the eligible contract with the highest AERY.
        """
        candidates = self.contract_chain.get_contracts_expiring_after(
            trade_date,
            min_days=self.min_roll_days,
        )

        best_aery = -np.inf
        best_contract: Optional[FuturesContract] = None

        for contract in candidates:
            aery = self._calculate_annualized_roll_yield(
                contract,
                snapshot,
                self.signal_price_field,
            )

            if aery is not None and aery > best_aery:
                best_aery = aery
                best_contract = contract

        return best_contract

    def _select_roll_target(
        self,
        snapshot: SignalSnapshot,
        current_contract: FuturesContract,
    ) -> Optional[FuturesContract]:
        """
        Override Baseline roll target selection.

        Returns the daily precomputed optimal AERY contract.
        NOTE: The cache is guaranteed to be updated in on_bar.
        """
        return self._optimal_roll_target

    def on_bar(
        self,
        snapshot: SignalSnapshot,
        account: Account,
    ) -> Dict[str, int]:
        """
        Override on_bar to combine AERY-based target selection
        with a fixed roll trigger.
        """
        trade_date = snapshot.trade_date

        # 1. Compute and cache the optimal AERY contract (decision layer)
        self._optimal_roll_target = self._select_optimal_target(trade_date, snapshot)

        # 2. Delegate execution logic to BaselineRollStrategy:
        #    a) If no position exists, it calls _select_contract().
        #    b) If a position exists, it evaluates _should_roll()
        #       based on roll_days_before_expiry.
        #    c) If a roll is required, it calls _select_roll_target(),
        #       which returns the cached optimal AERY contract.
        #    d) Position sizing is handled by _calculate_target_volume().
        target_positions = super().on_bar(snapshot, account)

        # 3. Clear the cache after execution
        self._optimal_roll_target = None

        return target_positions

    # The original OptimalMaturityStrategy._should_roll override is intentionally disabled.
    # Rolling behavior is fully governed by BaselineRollStrategy's fixed-day logic.
