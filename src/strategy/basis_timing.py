"""
Basis timing strategy - adds basis signals to baseline rolling.
"""
from datetime import date
from typing import Dict, Optional, List
from collections import deque
from loguru import logger

from ..domain.contract import FuturesContract
from ..domain.chain import ContractChain
from ..data.signal_snapshot import SignalSnapshot
from ..account.account import Account
from .baseline_roll import BaselineRollStrategy


class BasisTimingStrategy(BaselineRollStrategy):
    """
    Enhanced strategy that adds basis timing signals to baseline rolling.
    
    Idea:
    - When basis is deeply discounted (negative), go full position
    - When basis narrows or turns premium, reduce or close position
    - Optionally select contract with larger discount
    """
    
    def __init__(
        self,
        contract_chain: ContractChain,
        roll_days_before_expiry: int = 2,
        contract_selection: str = 'nearby',
        target_leverage: float = 1.0,
        min_roll_days: int = 5,
        signal_price_field: str = "open",  # Price field for signal calculation
        # Basis timing parameters
        basis_entry_threshold: float = -0.02,   # -2% discount to enter
        basis_exit_threshold: float = 0.005,    # +0.5% to exit
        lookback_window: int = 60,              # Days for percentile calculation
        use_percentile: bool = False,           # Use percentile instead of absolute
        entry_percentile: float = 0.2,          # Enter when basis < 20th percentile
        exit_percentile: float = 0.8,           # Exit when basis > 80th percentile
        position_scale_by_basis: bool = False,  # Scale position by basis depth
    ):
        super().__init__(
            contract_chain=contract_chain,
            roll_days_before_expiry=roll_days_before_expiry,
            contract_selection=contract_selection,
            target_leverage=target_leverage,
            min_roll_days=min_roll_days,
            signal_price_field=signal_price_field,
        )
        
        self.basis_entry_threshold = basis_entry_threshold
        self.basis_exit_threshold = basis_exit_threshold
        self.lookback_window = lookback_window
        self.use_percentile = use_percentile
        self.entry_percentile = entry_percentile
        self.exit_percentile = exit_percentile
        self.position_scale_by_basis = position_scale_by_basis
        
        # Basis history for percentile calculation
        self._basis_history: deque = deque(maxlen=lookback_window)
        self._position_state: str = "OUT"  # 'OUT', 'IN'
    
    def on_bar(
        self,
        snapshot: SignalSnapshot,
        account: Account
    ) -> Dict[str, int]:
        """
        Generate target positions with basis timing.
        """
        trade_date = snapshot.trade_date
        target_positions: Dict[str, int] = {}
        
        # Get base target from parent strategy
        base_targets = super().on_bar(snapshot, account)
        
        if not base_targets:
            return {}
        
        # Get the contract we're trading
        ts_code = list(base_targets.keys())[0] if base_targets else None
        if ts_code is None:
            return {}
        
        base_volume = base_targets.get(ts_code, 0)
        
        # Calculate current basis using SignalSnapshot
        # This ensures we CANNOT use T-day close (lookahead bias prevention)
        basis = snapshot.get_basis(ts_code, relative=True)
        if basis is None:
            # No basis info, use base strategy
            return base_targets
        
        # Record basis history
        self._basis_history.append(basis)
        
        # Determine signal
        signal = self._get_timing_signal(basis)
        
        # Adjust volume based on signal
        if signal == "ENTER":
            if self._position_state == "OUT":
                logger.info(f"Basis timing: ENTER on {trade_date}, basis={basis:.4f}")
            self._position_state = "IN"
            adjusted_volume = self._adjust_volume_by_basis(base_volume, basis)
            target_positions[ts_code] = adjusted_volume
            
        elif signal == "EXIT":
            if self._position_state == "IN":
                logger.info(f"Basis timing: EXIT on {trade_date}, basis={basis:.4f}")
            self._position_state = "OUT"
            target_positions[ts_code] = 0  # Exit
            
        else:  # HOLD
            if self._position_state == "IN":
                adjusted_volume = self._adjust_volume_by_basis(base_volume, basis)
                target_positions[ts_code] = adjusted_volume
            else:
                target_positions[ts_code] = 0  # Stay out
        
        # Handle rolling: if there's a close order for old contract
        for old_ts_code, vol in base_targets.items():
            if old_ts_code != ts_code and vol == 0:
                target_positions[old_ts_code] = 0
        
        return target_positions
    
    def _get_timing_signal(self, basis: float) -> str:
        """
        Get timing signal based on basis.
        Returns: 'ENTER', 'EXIT', or 'HOLD'
        """
        if self.use_percentile and len(self._basis_history) >= 20:
            percentile = self._calculate_percentile(basis)
            
            if percentile <= self.entry_percentile:
                return "ENTER"
            elif percentile >= self.exit_percentile:
                return "EXIT"
            else:
                return "HOLD"
        else:
            # Use absolute thresholds
            if basis <= self.basis_entry_threshold:
                return "ENTER"
            elif basis >= self.basis_exit_threshold:
                return "EXIT"
            else:
                return "HOLD"
    
    def _calculate_percentile(self, basis: float) -> float:
        """
        Calculate percentile of current basis in historical distribution.
        Lower percentile = deeper discount.
        """
        history = list(self._basis_history)
        if not history:
            return 0.5
        
        count_below = sum(1 for b in history if b < basis)
        return count_below / len(history)
    
    def _adjust_volume_by_basis(self, base_volume: int, basis: float) -> int:
        """
        Optionally scale volume by basis depth.
        Deeper discount -> larger position.
        """
        if not self.position_scale_by_basis:
            return base_volume
        
        # Scale factor: deeper discount -> larger position
        # At -5% discount: scale = 1.5
        # At 0% discount: scale = 0.5
        scale = 1.0 + (-basis) * 10  # Linear scaling
        scale = max(0.5, min(1.5, scale))  # Clamp to [0.5, 1.5]
        
        return int(base_volume * scale)
    
    def select_best_discount_contract(
        self,
        snapshot: SignalSnapshot,
        min_liquidity_volume: float = 1000,
    ) -> Optional[str]:
        """
        Select the contract with largest discount among liquid contracts.
        Can be used as an alternative contract selection rule.
        """
        trade_date = snapshot.trade_date
        candidates = self.contract_chain.get_contracts_expiring_after(
            trade_date,
            min_days=self.min_roll_days
        )
        
        # Filter by liquidity using T-1 volume from SignalSnapshot to avoid lookahead
        liquid_contracts = [
            c for c in candidates
            if (snapshot.get_prev_volume(c.ts_code) or 0.0) >= min_liquidity_volume
        ]
        
        if not liquid_contracts:
            return None
        
        # Calculate basis for each
        best_contract = None
        best_basis = float('inf')
        
        for contract in liquid_contracts:
            basis = snapshot.get_basis(contract.ts_code, relative=True)
            if basis is not None and basis < best_basis:
                best_basis = basis
                best_contract = contract
        
        return best_contract.ts_code if best_contract else None
