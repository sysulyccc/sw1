# Code Review Report

## 1. Future Information Leakage Prevention (UPDATED)

### 1.1 SignalSnapshot Design

The system now uses a **two-snapshot architecture** to prevent lookahead bias:

```
SignalSnapshot (for strategy)     MarketSnapshot (for execution)
├── T-day open                    ├── T-day open/close/settle
├── T-day pre_settle              ├── T-day volume/oi
├── T-day index open              ├── T-day index close
└── T-1 day complete data         └── All fields available
```

**Key changes**:
1. Strategy receives `SignalSnapshot` - CANNOT access T-day close, settle, volume
2. Execution uses full `MarketSnapshot` - for trade execution at configured price
3. Mark-to-market uses full snapshot - settle price (correct behavior)

### 1.2 Price Data Access Control

| Component | Snapshot Type | Available Fields | Risk Level |
|-----------|--------------|------------------|------------|
| `on_bar()` | **SignalSnapshot** | open, pre_settle only | **None** |
| `rebalance_to_target()` | MarketSnapshot | execution_price_field | None |
| `mark_to_market()` | MarketSnapshot | settle price | None |
| `get_basis()` | **SignalSnapshot** | open prices only | **None** |

**Result**: Strategy CANNOT use T-day close, settle, or volume for signal generation.

### 1.2 Contract Selection

```python
# baseline_roll.py:159
return max(candidates, key=lambda c: c.get_volume(trade_date))
```

**Issue**: Uses same-day volume to select contract. Volume is only known after market close.

**Risk**: Low - Volume ranking rarely changes intraday for main contracts.

### 1.3 Roll Decision

```python
# baseline_roll.py:117-118
def _should_roll(self, contract: FuturesContract, trade_date: date) -> bool:
    days_to_expiry = contract.days_to_expiry(trade_date)
    return days_to_expiry <= self.roll_days_before_expiry
```

**Assessment**: Safe - Uses calendar days to expiry (static info), no future data.

---

## 2. Bugs and Issues Found

### 2.1 Fixed Issues (Previous Session)

1. **Settle Price = 0 Bug** (CRITICAL - Fixed)
   - Location: `handler.py:165`
   - Issue: `settle=row["settle"] or 0.0` set settle to 0 when None
   - Impact: Caused basis = -1.0, massive losses
   - Fix: Use close price as fallback

2. **Invalid Basis Calculation** (CRITICAL - Fixed)
   - Location: `snapshot.py:56-63`
   - Issue: No validation for zero/None prices
   - Fix: Added price validity checks

### 2.2 Remaining Issues

#### Issue 1: Calendar Days vs Trading Days

**Location**: `contract.py:85-90`
```python
def days_to_expiry(self, trade_date: date) -> int:
    """Note: This returns calendar days. For trading days, need calendar."""
    return (self.delist_date - trade_date).days
```

**Problem**: Uses calendar days, not trading days. A 2-day roll window over a long weekend could miss the roll.

**Recommendation**: Add trading days calculation:
```python
def trading_days_to_expiry(self, trade_date: date, calendar: List[date]) -> int:
    return sum(1 for d in calendar if trade_date < d <= self.delist_date)
```

#### Issue 2: No Slippage Model

**Location**: `account.py:170`
```python
price = snapshot.get_futures_price(ts_code, 'settle')
```

**Problem**: Assumes perfect execution at settle price. Real trading has slippage.

**Recommendation**: Add configurable slippage:
```python
slippage = price * self.slippage_rate  # e.g., 0.0001
execution_price = price + slippage if buying else price - slippage
```

#### Issue 3: No Position Limit Check

**Location**: `baseline_roll.py:183-185`
```python
volume = int(target_notional / contract_value)
return max(volume, 0)  # Long only
```

**Problem**: No maximum position limit. Could over-leverage in extreme cases.

**Recommendation**: Add position limits in config.

---

## 3. Potential Data Quality Issues

### 3.1 Missing Settle Prices

Some historical records may have `settle=None`. Current fix uses `close` as fallback, which is reasonable but not perfect.

**Recommendation**: Log warnings when fallback is used:
```python
if settle is None or settle <= 0:
    logger.warning(f"Missing settle for {ts_code} on {date}, using close")
```

### 3.2 Index Data Alignment

**Location**: `handler.py:87`
```python
calendar = sorted(index_dates & futures_dates)
```

**Assessment**: Safe - Only uses dates where both index and futures data exist.

---

## 4. Security Review

### 4.1 No Sensitive Data Exposure

- No API keys or credentials in code
- No hardcoded passwords
- Data paths use relative or configurable paths

### 4.2 Output Files

Output files (CSV, PNG) contain only backtest results, no sensitive information.

---

## 5. Code Quality Issues

### 5.1 Type Hints

Most functions have proper type hints. 

### 5.2 Error Handling

Current approach: Return early with warning logs rather than raise exceptions.
This is acceptable for backtest systems where partial results are often useful.

### 5.3 Magic Numbers

Some thresholds are hardcoded:
- `basis_entry_threshold = -0.02` (config)
- `roll_days_before_expiry = 2` (config)

**Assessment**: OK - These are configurable via config.toml.

---

## 6. Recommendations Summary

| Priority | Issue | Recommendation |
|----------|-------|----------------|
| Medium | Calendar vs Trading Days | Implement trading days calculation |
| Low | No Slippage | Add slippage model |
| Low | No Position Limits | Add max position config |
| Info | Missing Data Warnings | Add logging for data fallbacks |

---

## 7. Conclusion

**Overall Assessment**: The codebase is well-structured with minimal future information leakage risk for a daily-frequency backtest system. The critical data bugs have been fixed. Remaining issues are enhancements rather than critical bugs.

**Information Leakage Risk**: **LOW**
- Using same-day settle price is standard practice for daily backtests
- No lookahead bias in signal generation logic
- Contract selection uses current-day observable data
