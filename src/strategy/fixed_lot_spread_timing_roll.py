"""
Fixed-lot spread timing roll strategy.
"""

from ..domain.contract import FuturesContract
from ..domain.chain import ContractChain
from ..data.signal_snapshot import SignalSnapshot
from ..account.account import Account
from .spread_timing_roll import SpreadTimingRollStrategy


class FixedLotSpreadTimingRollStrategy(SpreadTimingRollStrategy):
    def __init__(
        self,
        contract_chain: ContractChain,
        min_roll_days: int = 5,
        futures_price_field: str = "open",
        contract_selection: str = "nearby",
        roll_window_start: int = 15,
        hard_roll_days: int = 2,
        history_window: int = 90,
        spread_threshold_percentile: int = 30,
        fixed_lot_size: int = 1,
        **kwargs,
    ):
        super().__init__(
            contract_chain=contract_chain,
            target_leverage=1.0,
            min_roll_days=min_roll_days,
            futures_price_field=futures_price_field,
            contract_selection=contract_selection,
            roll_window_start=roll_window_start,
            hard_roll_days=hard_roll_days,
            history_window=history_window,
            spread_threshold_percentile=spread_threshold_percentile,
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
