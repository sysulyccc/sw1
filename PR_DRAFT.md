# PR: 交易逻辑增强与动态保证金实现

## 1. 变更摘要 (Summary)

本次提交完成了 **交易逻辑确认**、**核心引擎增强** 以及 **策略优化框架搭建**，主要包含：
- ✅ **智能移仓策略 (Smart Roll)**：新增基于成交量/持仓量 (Volume/OI) 的自动移仓策略，解决了固定日历移仓的流动性风险。
- ✅ **动态保证金机制 (Dynamic Margin)**：支持根据历史真实数据自动调整保证金率（如 2015 股灾期间自动升至 40%），替代原有的固定 12%。
- ✅ **现金结算逻辑修复 (Bug Fix)**：修复了平仓/换月时"昨结算"到"成交价"区间 PnL 未正确结算入账的问题，资金流水现在完全闭环。
- ✅ **全链路测试覆盖**：新增 `tests/test_trading_flow.py` 和 `tests/test_smart_roll.py`。
- ✅ **SmartRoll 策略增强**：
  - 使用**交易日历**计算剩余交易日（替代日历日），避免节假日导致的误判。
  - 添加 **5% 流动性阈值**，避免流动性接近时的乒乓换仓。
  - 添加**贴水检查**，确保只换到更便宜（贴水更深）的合约。
- ✅ **超额收益指标**：新增 Excess Max Drawdown、Excess Win Rate、Excess Calmar 等指数增强策略核心指标。

## 2. 详细改动 (Detailed Changes)

### 策略优化 (Strategy)
- **`src/strategy/smart_roll.py`**:
    - 新增 `SmartRollStrategy` 类，继承自 `BaselineRollStrategy`，符合 OCP 原则。
    - 实现 `_should_roll` 逻辑：监控次主力合约的流动性（Volume/OI），当其超越当前持仓合约时触发移仓。
    - 保底逻辑：若临近交割仍未触发切换，强制移仓以防交割风险。
    - **新增** `_trading_days_to_expiry` 方法：使用交易日历计算真实剩余交易日，避免节假日误判。
    - **新增** `liquidity_threshold` 参数（默认 5%）：流动性必须超过当前合约 5% 以上才触发换仓，避免乒乓换仓。
    - **新增** 贴水检查：只有当候选合约基差 ≤ 当前合约基差时才触发换仓，确保不换到更贵的合约。
- **`src/strategy/baseline_roll.py`**:
    - 重构 `_should_roll` 接口，增加 `snapshot` 参数，增强基类的扩展性。

### 分析器增强 (Analyzer)
- **`src/backtest/analyzer.py`**:
    - **新增** 超额收益指标：`excess_max_drawdown`（超额最大回撤）、`excess_win_rate`（超额胜率）、`excess_calmar`（超额卡尔马）。
    - **新增** 超额收益回撤可视化：在 Excess NAV 图上叠加超额回撤曲线。
    - **新增** 公式说明：在指标表格底部显示 Sharpe 和 IR 的计算公式。
    - 更新 `generate_report()` 输出格式，新增 "Excess Performance (Alpha)" 分组。

### 核心代码
- **`src/backtest/engine.py`**: 
    - 增加 `use_dynamic_margin` 参数支持。
    - 在每日 `_process_day` 流程中加入保证金率更新逻辑，优先读取 `DataHandler` 中的历史保证金数据。
- **`src/account/account.py`**: 
    - 修改 `_execute_trade` 方法：在更新持仓量（`update_volume`）之前，优先计算并结算当前价格与昨结算价之间的价差盈亏（Price Move PnL），确保资金计算准确。
- **`main.py`**: 
    - 支持 `strategy_type="smart_roll"` 配置及实例化。

### 测试与验证
- **新增 `tests/test_smart_roll.py`**:
    - 验证成交量/持仓量交叉时的自动触发逻辑。
    - 验证临近交割时的强制风控移仓。
- **新增 `tests/test_trading_flow.py`**:
    - `test_dynamic_margin_update`: 验证特殊日期（如 2015-09-07）保证金率是否正确跳变。
    - `test_intraday_timeline_correctness`: 验证早盘 Open 成交 -> 晚盘 Settle 盯市的流程闭环。
    - `test_roll_logic_execution`: 验证换月操作的资金流（佣金扣除 + 平仓结算 + 新仓开立）。

## 3. 验证结果 (Verification)

- **单元测试**: `pytest tests/` 全量通过。
- **逻辑一致性**: 新策略完全复用了基类的资金管理与信号执行逻辑，仅在“何时移仓”上做了优化。
- **回测结果 (Smart Roll + 贴水检查)**:
  ```
  Strategy Performance:
    Total Return:          168.12%
    Annualized Return:     9.72%
    Sharpe Ratio:          0.30
    Max Drawdown:          -46.82%

  Excess Performance (Alpha):
    Alpha (Ann.):          11.50%
    Information Ratio:     1.28
    Excess Max Drawdown:   -8.85%
    Excess Win Rate:       54.80%
    Excess Calmar:         1.30

  Trading Statistics:
    Total Trades:          483
  ```

## 4. 配置说明

- **`config.toml`** 新增参数：
  - `liquidity_threshold = 0.05`：流动性超越阈值（默认 5%）
  - `roll_criteria = "volume"`：换仓触发依据（volume 或 oi）
  - `roll_days_before_expiry = 2`：强制换仓的剩余交易日阈值

## 5. 给队友的 Note

- **@佩瑶 (Strategy)**: 以后跑 2015 年回测时，你会发现资金占用率（Margin Occupied）显著变大，这是符合历史事实的（当时保证金率极高），不是 Bug。
- **@然 (Doc/UML)**: `BacktestEngine` 现在与 `DataHandler` 有了关于 Margin Rate 的新交互，UML 图可能需要微调。`Analyzer` 新增了超额收益指标计算。
- **@卓/赖 (Data)**: 目前保证金数据 (`margin_ratio.parquet`) 已被核心引擎正式使用，请确保该数据文件的完整性。
