"""
Fixed-lot liquidity roll strategy.
"""

from typing import Literal

from ..domain.contract import FuturesContract
from ..domain.chain import ContractChain
from ..data.signal_snapshot import SignalSnapshot
from ..account.account import Account
from .liquidity_roll import LiquidityRollStrategy


class FixedLotLiquidityRollStrategy(LiquidityRollStrategy):
    def __init__(
        self,
        contract_chain: ContractChain,
        roll_days_before_expiry: int = 1,
        contract_selection: str = "nearby",
        fixed_lot_size: int = 1,
        min_roll_days: int = 5,
        signal_price_field: str = "open",
        roll_criteria: Literal["volume", "oi"] = "volume",
        **kwargs,
    ):
        super().__init__(
            contract_chain=contract_chain,
            roll_days_before_expiry=roll_days_before_expiry,
            contract_selection=contract_selection,
            target_leverage=1.0,
            min_roll_days=min_roll_days,
            signal_price_field=signal_price_field,
            roll_criteria=roll_criteria,
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
