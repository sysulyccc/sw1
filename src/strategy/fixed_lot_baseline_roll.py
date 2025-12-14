"""
Fixed-lot baseline roll strategy.
"""

from typing import Literal

from ..domain.contract import FuturesContract
from ..domain.chain import ContractChain
from ..data.signal_snapshot import SignalSnapshot
from ..account.account import Account
from .baseline_roll import BaselineRollStrategy


class FixedLotBaselineRollStrategy(BaselineRollStrategy):
    def __init__(
        self,
        contract_chain: ContractChain,
        roll_days_before_expiry: int = 2,
        contract_selection: Literal['nearby', 'next_nearby', 'volume', 'oi'] = 'nearby',
        fixed_lot_size: int = 1,
        min_roll_days: int = 5,
        signal_price_field: str = "open",
    ):
        super().__init__(
            contract_chain=contract_chain,
            roll_days_before_expiry=roll_days_before_expiry,
            contract_selection=contract_selection,
            target_leverage=1.0,
            min_roll_days=min_roll_days,
            signal_price_field=signal_price_field,
        )
        self.position_mode = "fixed_lot"
        self.fixed_lot_size = fixed_lot_size

    def _calculate_target_volume(
        self,
        contract: FuturesContract,
        snapshot: SignalSnapshot,
        account: Account,
    ) -> int:
        price = snapshot.get_futures_price(contract.ts_code, self.signal_price_field)
        if price is None or price <= 0:
            return 0
        return max(int(self.fixed_lot_size), 0)
