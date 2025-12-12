"""
Smart roll strategy - liquidity driven rolling.
"""
from datetime import date
from typing import Optional, Literal, List
from loguru import logger

from ..domain.contract import FuturesContract
from ..domain.chain import ContractChain
from ..data.signal_snapshot import SignalSnapshot
from .baseline_roll import BaselineRollStrategy


class SmartRollStrategy(BaselineRollStrategy):
    """
    Smart rolling strategy based on liquidity crossover.
    
    Logic:
    1. Monitor the next dominant contract (usually next month or next quarter).
    2. Roll when the next contract's Volume or Open Interest exceeds the current holding.
    3. Safety: Force roll if days to expiry is too small (e.g., 1 day), regardless of liquidity.
    """
    
    def __init__(
        self,
        contract_chain: ContractChain,
        roll_days_before_expiry: int = 2,  # Force roll if <= N trading days left
        contract_selection: str = 'nearby', # Initial selection
        target_leverage: float = 1.0,
        min_roll_days: int = 5,
        signal_price_field: str = "open",
        roll_criteria: Literal['volume', 'oi'] = 'volume', # Trigger criteria
        liquidity_threshold: float = 0.05,  # 5% threshold to avoid ping-pong rolling
        trading_calendar: Optional[List[date]] = None,  # Trading calendar for accurate day counting
    ):
        super().__init__(
            contract_chain=contract_chain,
            roll_days_before_expiry=roll_days_before_expiry,
            contract_selection=contract_selection,
            target_leverage=target_leverage,
            min_roll_days=min_roll_days,
            signal_price_field=signal_price_field
        )
        self.roll_criteria = roll_criteria
        self.liquidity_threshold = liquidity_threshold
        self.trading_calendar = trading_calendar
        self._check_next_contract: Optional[FuturesContract] = None
    
    def _trading_days_to_expiry(self, contract: FuturesContract, trade_date: date) -> int:
        """
        Calculate trading days to expiry using calendar.
        Falls back to calendar days if no trading calendar provided.
        """
        if self.trading_calendar is None:
            # Fallback to calendar days
            return contract.days_to_expiry(trade_date)
        
        # Count trading days between trade_date and delist_date
        count = 0
        for d in self.trading_calendar:
            if trade_date < d <= contract.delist_date:
                count += 1
        return count

    def _should_roll(self, contract: FuturesContract, snapshot: SignalSnapshot) -> bool:
        """
        Check if we should roll based on liquidity crossover or forced expiry.
        """
        trade_date = snapshot.trade_date
        
        # 1. Safety Check: Force roll if very close to expiry (using trading days)
        trading_days_left = self._trading_days_to_expiry(contract, trade_date)
        if trading_days_left <= self.roll_days_before_expiry:
            logger.info(f"Force rolling {contract.ts_code}: trading_days_to_expiry={trading_days_left}")
            return True
            
        # 2. Identify the candidate for liquidity comparison
        candidates = self.contract_chain.get_contracts_expiring_after(
            trade_date, 
            min_days=self.min_roll_days
        )
        # Filter out current contract
        candidates = [c for c in candidates if c.ts_code != contract.ts_code]
        
        if not candidates:
            return False
            
        # The most likely roll target is the first one (next expiry)
        candidate = candidates[0]
        
        # 3. Liquidity Comparison (using T-1 data from snapshot)
        current_val = 0.0
        candidate_val = 0.0
        
        if self.roll_criteria == 'volume':
            current_val = snapshot.get_prev_volume(contract.ts_code) or 0.0
            candidate_val = snapshot.get_prev_volume(candidate.ts_code) or 0.0
        elif self.roll_criteria == 'oi':
            current_val = snapshot.get_prev_oi(contract.ts_code) or 0.0
            candidate_val = snapshot.get_prev_oi(candidate.ts_code) or 0.0
            
        # 4. Trigger roll only if candidate exceeds current by threshold (avoid ping-pong)
        if current_val > 0 and candidate_val > current_val * (1 + self.liquidity_threshold):
            # 5. Basis Check: Only roll if candidate is not more expensive (贴水更深或相等)
            current_basis = snapshot.get_basis(contract.ts_code, relative=True)
            candidate_basis = snapshot.get_basis(candidate.ts_code, relative=True)
            
            if current_basis is not None and candidate_basis is not None:
                # 贴水为负值，candidate_basis <= current_basis 表示候选合约更便宜或一样
                if candidate_basis > current_basis:
                    logger.debug(f"Roll blocked: {candidate.ts_code} basis ({candidate_basis:.4f}) > {contract.ts_code} basis ({current_basis:.4f})")
                    return False
            
            logger.info(f"Liquidity roll triggered: {candidate.ts_code} ({candidate_val:.0f}) > {contract.ts_code} ({current_val:.0f}) by {(candidate_val/current_val - 1)*100:.1f}%")
            return True
            
        return False

    # No need to override on_bar anymore since base class handles passing snapshot
