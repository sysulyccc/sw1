from datetime import date
from typing import Dict, Optional, List
from loguru import logger
import numpy as np
from collections import deque

from ..domain.contract import FuturesContract
from ..domain.chain import ContractChain
from ..data.signal_snapshot import SignalSnapshot
from ..account.account import Account
from .baseline_roll import BaselineRollStrategy


class SpreadTimingRollStrategy(BaselineRollStrategy):
    """
    Spread-timed roll strategy based on inter-contract price differentials.

    Objectives:
    1. Within a predefined window before contract expiration, identify
       favorable roll timing based on historical percentiles of the
       inter-month spread.
    2. Roll target selection (e.g., nearby, open interest, volume) follows
       the inherited `contract_selection` logic from BaselineRollStrategy.
    """

    def __init__(
        self,
        contract_chain: ContractChain,
        target_leverage: float = 1.0,
        min_roll_days: int = 5,
        futures_price_field: str = "open",
        contract_selection: str = "nearby",
        roll_window_start: int = 15,           # Roll window start before expiry (days)
        hard_roll_days: int = 2,               # Forced roll threshold (days to expiry)
        history_window: int = 90,              # Lookback window for spread history
        spread_threshold_percentile: int = 30, # Roll if spread is below this percentile
        **kwargs,
    ):
        # Store strategy-specific parameters
        self.roll_window_start = roll_window_start
        self.hard_roll_days = hard_roll_days
        self.history_window = history_window
        self.spread_threshold_percentile = spread_threshold_percentile

        # Initialize baseline strategy with consistent contract selection
        super().__init__(
            contract_chain,
            roll_days_before_expiry=self.roll_window_start,
            contract_selection=contract_selection,
            target_leverage=target_leverage,
            min_roll_days=min_roll_days,
            signal_price_field=futures_price_field,
        )

        # Maintain historical inter-month spread and roll cost metrics
        self._spread_history: deque = deque(maxlen=self.history_window)
        self._roll_cost_history: deque = deque(maxlen=self.history_window)

    def _calculate_spread_and_cost(
        self,
        F_current: float,
        F_target: float,
        days_to_target_expiry: int,
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Compute inter-month spread and annualized roll cost.

        Spread = F_current - F_target
        Roll cost (annualized) = Spread / F_current * (365 / days_to_target_expiry)
        """
        if F_current is None or F_target is None or days_to_target_expiry <= 0:
            return None, None

        spread = F_current - F_target
        cost_rate = spread / F_current * (365 / days_to_target_expiry)

        return spread, cost_rate

    def on_bar(
        self,
        snapshot: SignalSnapshot,
        account: Account,
    ) -> Dict[str, int]:
        """
        Execute roll decisions based on spread timing signals.
        """
        trade_date = snapshot.trade_date
        target_positions: Dict[str, int] = {}

        # 1. Initial entry: no existing position
        holding_contracts = account.get_holding_contracts()
        if not holding_contracts:
            contract = self._select_contract(snapshot)
            if contract is None:
                return {}

            self._current_contract = contract
            volume = self._calculate_target_volume(contract, snapshot, account)
            target_positions[contract.ts_code] = volume
            return target_positions

        current_ts_code = holding_contracts[0]
        current_contract = self.contract_chain.get_contract(current_ts_code)
        if current_contract is None:
            return {}

        self._current_contract = current_contract

        F_current = snapshot.get_futures_price(
            current_ts_code, self.signal_price_field
        )
        if F_current is None:
            return {}

        # 2. Identify nearest eligible contract for spread calculation
        candidates = self._get_tradable_candidates(trade_date, current_ts_code)
        spread_to_next = None

        if candidates:
            next_contract = candidates[0]
            F_next = snapshot.get_futures_price(
                next_contract.ts_code, self.signal_price_field
            )

            if F_next is not None:
                spread_to_next, cost_rate = self._calculate_spread_and_cost(
                    F_current,
                    F_next,
                    next_contract.days_to_expiry(trade_date),
                )

                if spread_to_next is not None:
                    self._spread_history.append(spread_to_next)
                if cost_rate is not None:
                    self._roll_cost_history.append(cost_rate)

        # 3. Roll trigger evaluation
        days_to_expiry = current_contract.days_to_expiry(trade_date)
        should_roll_now = False

        # A. Forced roll close to expiration
        if days_to_expiry <= self.hard_roll_days:
            should_roll_now = True

        # B. Spread-timed roll within the roll window
        elif days_to_expiry <= self.roll_window_start:
            if (
                len(self._spread_history) >= self.history_window / 2
                and spread_to_next is not None
            ):
                history = list(self._spread_history)
                threshold = np.percentile(
                    history, self.spread_threshold_percentile
                )

                if spread_to_next <= threshold:
                    should_roll_now = True
            else:
                # Insufficient history: force roll within window
                should_roll_now = True

        # 4. Execute roll or maintain position
        if should_roll_now:
            new_contract = self._select_roll_target(snapshot, current_contract)

            if new_contract is None:
                volume = self._calculate_target_volume(
                    current_contract, snapshot, account
                )
                target_positions[current_ts_code] = volume
                return target_positions

            target_positions[current_ts_code] = 0
            volume = self._calculate_target_volume(
                new_contract, snapshot, account
            )
            target_positions[new_contract.ts_code] = volume
            self._current_contract = new_contract

        else:
            volume = self._calculate_target_volume(
                current_contract, snapshot, account
            )
            target_positions[current_ts_code] = volume

        return target_positions

    # ------------------------------------------------------------------
    # Helper methods

    def _get_tradable_candidates(
        self,
        trade_date: date,
        current_ts_code: str,
    ) -> List[FuturesContract]:
        """
        Return eligible future contracts excluding the current holding.
        """
        candidates = self.contract_chain.get_contracts_expiring_after(
            trade_date,
            min_days=self.min_roll_days,
        )
        return [c for c in candidates if c.ts_code != current_ts_code]
