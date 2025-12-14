from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd

from ..account.account import TradeRecord


@dataclass(frozen=True)
class TradeSeries:
    total_volume: pd.Series
    total_notional: pd.Series
    buy_points: List[Tuple[pd.Timestamp, float]]
    sell_points: List[Tuple[pd.Timestamp, float]]


class TradeLogPlotter:
    def build_trade_series(
        self,
        nav_series: pd.Series,
        trade_log: Iterable[TradeRecord],
    ) -> TradeSeries:
        nav_series = nav_series.sort_index()
        dates = [ts.normalize() for ts in nav_series.index]
        if not dates:
            empty = pd.Series(dtype=float)
            return TradeSeries(
                total_volume=empty,
                total_notional=empty,
                buy_points=[],
                sell_points=[],
            )

        by_date: Dict[date, List[TradeRecord]] = {}
        for t in trade_log:
            by_date.setdefault(t.trade_date, []).append(t)

        vol_by_contract: Dict[str, int] = {}
        last_price: Dict[str, float] = {}
        multiplier: Dict[str, float] = {}

        total_volume: List[float] = []
        total_notional: List[float] = []

        buy_points: List[Tuple[pd.Timestamp, float]] = []
        sell_points: List[Tuple[pd.Timestamp, float]] = []

        nav_lookup = nav_series.copy()

        def _nav_at(ts: pd.Timestamp) -> Optional[float]:
            if ts in nav_lookup.index:
                v = nav_lookup.loc[ts]
                return float(v) if pd.notna(v) else None
            idx = nav_lookup.index.get_indexer([ts], method="pad")
            if len(idx) == 0 or idx[0] < 0:
                return None
            v = nav_lookup.iloc[int(idx[0])]
            return float(v) if pd.notna(v) else None

        for ts in dates:
            d = ts.date()
            for t in by_date.get(d, []):
                signed = t.volume if t.direction == "BUY" else -t.volume
                vol_by_contract[t.ts_code] = vol_by_contract.get(t.ts_code, 0) + signed
                if t.price is not None and t.price > 0:
                    last_price[t.ts_code] = float(t.price)
                if t.volume > 0 and t.price > 0:
                    m = float(t.amount) / float(t.volume * t.price)
                    if m > 0:
                        multiplier[t.ts_code] = m

                y = _nav_at(pd.Timestamp(d))
                if y is not None:
                    if t.direction == "BUY":
                        buy_points.append((pd.Timestamp(d), y))
                    else:
                        sell_points.append((pd.Timestamp(d), y))

            total_vol_today = float(sum(vol_by_contract.values()))
            total_notional_today = 0.0
            for ts_code, vol in vol_by_contract.items():
                px = last_price.get(ts_code)
                mul = multiplier.get(ts_code)
                if px is None or mul is None:
                    continue
                total_notional_today += abs(float(vol)) * float(px) * float(mul)

            total_volume.append(total_vol_today)
            total_notional.append(total_notional_today)

        idx = pd.DatetimeIndex(dates)
        return TradeSeries(
            total_volume=pd.Series(total_volume, index=idx),
            total_notional=pd.Series(total_notional, index=idx),
            buy_points=buy_points,
            sell_points=sell_points,
        )

    def plot_to_file(
        self,
        nav_series: pd.Series,
        trade_log: Iterable[TradeRecord],
        output_path: str | Path,
        strategy_name: str,
        dpi: int = 150,
    ) -> None:
        output_path = Path(output_path)
        s = self.build_trade_series(nav_series, trade_log)
        if nav_series.empty:
            return

        fig, (ax1, ax2) = plt.subplots(
            nrows=2,
            ncols=1,
            figsize=(16, 10),
            dpi=dpi,
            sharex=True,
            gridspec_kw={"height_ratios": [2, 1]},
        )

        nav_series = nav_series.sort_index()
        ax1.plot(
            nav_series.index,
            nav_series.values,
            linewidth=1.5,
            color="#2E86AB",
            label=f"{strategy_name} NAV",
        )

        if s.buy_points:
            xs, ys = zip(*s.buy_points)
            ax1.scatter(xs, ys, s=18, marker="^", color="#2CA02C", label="BUY")
        if s.sell_points:
            xs, ys = zip(*s.sell_points)
            ax1.scatter(xs, ys, s=18, marker="v", color="#D62728", label="SELL")

        ax1.set_ylabel("NAV")
        ax1.set_title("NAV with Trade Points")
        ax1.grid(True, alpha=0.3)
        ax1.legend(loc="upper left")

        ax2.step(
            s.total_volume.index,
            s.total_volume.values,
            where="post",
            linewidth=1.2,
            color="#9467BD",
            label="Total Volume (Lots)",
        )
        ax2.set_ylabel("Lots")
        ax2.grid(True, alpha=0.3)

        ax2b = ax2.twinx()
        ax2b.plot(
            s.total_notional.index,
            s.total_notional.values,
            linewidth=1.0,
            color="#FF7F0E",
            alpha=0.8,
            label="Approx Notional",
        )
        ax2b.set_ylabel("Notional")

        lines, labels = ax2.get_legend_handles_labels()
        lines2, labels2 = ax2b.get_legend_handles_labels()
        ax2.legend(lines + lines2, labels + labels2, loc="upper left")

        fig.suptitle(f"{strategy_name} Trade Log", fontsize=14, fontweight="bold")
        fig.tight_layout()
        fig.savefig(output_path, dpi=dpi, bbox_inches="tight")
        plt.close(fig)
