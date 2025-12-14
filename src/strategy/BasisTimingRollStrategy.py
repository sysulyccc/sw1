from datetime import date
from typing import Dict, Optional
from loguru import logger
import numpy as np
from collections import deque

from ..domain.contract import FuturesContract
from ..domain.chain import ContractChain
from ..data.signal_snapshot import SignalSnapshot
from ..account.account import Account
from . import BaselineRollStrategy


class BasisTimingRollStrategy(BaselineRollStrategy):
    """
    Basis-timed roll strategy based on futures–spot spread.

    Objectives:
    1. Within the roll window prior to contract expiry, execute a roll only
       when the basis of the current (nearby) contract is at a favorable
       historical percentile.
    2. The roll target selection logic (e.g., nearby, open interest, volume)
       is determined by the inherited `contract_selection` parameter.
    """

    def __init__(
        self,
        contract_chain: ContractChain,
        target_leverage: float = 1.0,
        min_roll_days: int = 5,
        futures_price_field: str = "open",
        roll_window_start: int = 15,          # Start of roll window before expiry (days)
        hard_roll_days: int = 1,              # Forced roll threshold (days to expiry)
        history_window: int = 60,             # Lookback window for basis history
        basis_threshold_percentile: int = 70, # Required percentile to trigger roll
        contract_selection: str = "oi",
        **kwargs
    ):
        # Store strategy-specific parameters
        self.roll_window_start = roll_window_start
        self.hard_roll_days = hard_roll_days
        self.history_window = history_window
        self.basis_threshold_percentile = basis_threshold_percentile

        # Initialize the baseline strategy
        # Note:
        # Although roll timing is fully controlled by on_bar logic,
        # roll_days_before_expiry is still passed to the base class
        # to maintain internal consistency.
        super().__init__(
            contract_chain,
            roll_days_before_expiry=self.roll_window_start,
            contract_selection=contract_selection,
            target_leverage=target_leverage,
            min_roll_days=min_roll_days,
            signal_price_field=futures_price_field,
        )

        # Maintain rolling history of basis values (Basis = F - S)
        self._basis_history: deque = deque(maxlen=self.history_window)

    def _calculate_basis(
        self,
        trade_date: date,
        current_contract: FuturesContract,
        snapshot: SignalSnapshot,
    ) -> Optional[float]:
        """
        Compute the basis between the current futures contract and the index.

        Basis = F_nearby - S_index
        """
        futures_price = snapshot.get_futures_price(
            current_contract.ts_code, self.signal_price_field
        )
        index_price = snapshot.get_index_price(self.signal_price_field)

        if futures_price is None or index_price is None:
            return None

        return futures_price - index_price

    def on_bar(
        self,
        snapshot: SignalSnapshot,
        account: Account,
    ) -> Dict[str, int]:
        """
        Execute roll decisions based on basis timing signals.
        """
        trade_date = snapshot.trade_date
        target_positions: Dict[str, int] = {}

        # 1. Initial entry: no existing position
        holding_contracts = account.get_holding_contracts()
        if not holding_contracts:
            return self._handle_initial_position(
                trade_date, snapshot, account, target_positions
            )

        current_ts_code = holding_contracts[0]
        current_contract = self.contract_chain.get_contract(current_ts_code)
        if current_contract is None:
            return {}

        # 2. Update basis history
        current_basis = self._calculate_basis(trade_date, current_contract, snapshot)
        if current_basis is not None:
            self._basis_history.append(current_basis)

        # 3. Roll trigger evaluation
        days_to_expiry = current_contract.days_to_expiry(trade_date)
        should_roll_now = False

        # A. Hard roll rule
        if days_to_expiry <= self.hard_roll_days:
            should_roll_now = True

        # B. Basis-timed roll within the roll window
        elif days_to_expiry <= self.roll_window_start:
            if len(self._basis_history) >= self.history_window / 2:
                history = list(self._basis_history)
                threshold = np.percentile(
                    history, self.basis_threshold_percentile
                )

                if current_basis is not None and current_basis >= threshold:
                    should_roll_now = True
            else:
                # Insufficient history: force roll as a fallback
                should_roll_now = True

        # 4. Execute roll or maintain position
        if should_roll_now:
            new_contract = self._select_roll_target(trade_date, current_contract)

            if new_contract is None:
                volume = self._calculate_target_volume(
                    current_contract, snapshot, account
                )
                target_positions[current_ts_code] = volume
                return target_positions

            # Close old position and open new one
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
            self._current_contract = current_contract

        return target_positions

    # ------------------------------------------------------------------
    # Helper method: ensure initial entry follows base contract selection

    def _handle_initial_position(
        self,
        trade_date,
        snapshot,
        account,
        target_positions,
    ):
        """
        Handle initial position entry using the base contract selection logic.
        """
        contract = self._select_contract(trade_date)

        if contract is None:
            return {}

        self._current_contract = contract
        volume = self._calculate_target_volume(contract, snapshot, account)
        target_positions[contract.ts_code] = volume
        return target_positions

    # _select_roll_target is intentionally inherited from BaselineRollStrategy.
