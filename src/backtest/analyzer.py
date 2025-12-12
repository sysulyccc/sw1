"""
Analyzer - computes performance metrics and generates reports.
"""
from pathlib import Path
from typing import Dict, List, Optional
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.gridspec import GridSpec

from ..account.account import TradeRecord
from ..config import TRADING_DAYS_PER_YEAR


class Analyzer:
    """
    Performance analyzer for backtest results.
    Computes risk-return metrics and generates visualization.
    """
    
    def __init__(
        self,
        nav_series: pd.Series,
        benchmark_nav: pd.Series,
        trade_log: Optional[List[TradeRecord]] = None,
        strategy_name: str = "Strategy",
        benchmark_name: str = "Benchmark",
        risk_free_rate: float = 0.02,
        trading_days_per_year: int = TRADING_DAYS_PER_YEAR,
    ):
        """
        Args:
            nav_series: Strategy NAV series (indexed by date)
            benchmark_nav: Benchmark NAV series (indexed by date)
            trade_log: List of trade records
            strategy_name: Name of the strategy for display
            benchmark_name: Name of the benchmark for display
            risk_free_rate: Annual risk-free rate for Sharpe calculation
            trading_days_per_year: Number of trading days per year (242 for China)
        """
        self.nav_series = nav_series
        self.benchmark_nav = benchmark_nav
        self.trade_log = trade_log or []
        self.strategy_name = strategy_name
        self.benchmark_name = benchmark_name
        self.risk_free_rate = risk_free_rate
        self.trading_days_per_year = trading_days_per_year
        
        self._align_series()
    
    def _align_series(self) -> None:
        """Align NAV and benchmark series to common dates."""
        common_dates = self.nav_series.index.intersection(self.benchmark_nav.index)
        self.nav_series = self.nav_series.loc[common_dates]
        self.benchmark_nav = self.benchmark_nav.loc[common_dates]
    
    def compute_metrics(self) -> Dict[str, float]:
        """
        Compute all performance metrics.
        Returns: Dict of metric name -> value
        """
        tdy = self.trading_days_per_year
        
        strategy_returns = self.nav_series.pct_change().dropna()
        benchmark_returns = self.benchmark_nav.pct_change().dropna()
        
        total_return = self.nav_series.iloc[-1] / self.nav_series.iloc[0] - 1
        benchmark_total = self.benchmark_nav.iloc[-1] / self.benchmark_nav.iloc[0] - 1
        
        n_days = len(self.nav_series)
        n_years = n_days / tdy
        
        ann_return = (1 + total_return) ** (1 / n_years) - 1 if n_years > 0 else 0
        benchmark_ann = (1 + benchmark_total) ** (1 / n_years) - 1 if n_years > 0 else 0
        
        ann_vol = strategy_returns.std() * np.sqrt(tdy)
        benchmark_vol = benchmark_returns.std() * np.sqrt(tdy)
        
        excess_return = ann_return - self.risk_free_rate
        sharpe = excess_return / ann_vol if ann_vol > 0 else 0
        
        cummax = self.nav_series.cummax()
        drawdown = (self.nav_series - cummax) / cummax
        max_dd = drawdown.min()
        
        alpha = ann_return - benchmark_ann
        
        excess_returns = strategy_returns - benchmark_returns
        tracking_error = excess_returns.std() * np.sqrt(tdy)
        info_ratio = alpha / tracking_error if tracking_error > 0 else 0
        
        win_rate = (strategy_returns > 0).mean()
        calmar = ann_return / abs(max_dd) if max_dd != 0 else 0
        
        # === Excess Return Metrics (for Index Enhancement) ===
        excess_nav = self.nav_series / self.benchmark_nav
        excess_cummax = excess_nav.cummax()
        excess_drawdown = (excess_nav - excess_cummax) / excess_cummax
        excess_max_dd = excess_drawdown.min()
        
        excess_win_rate = (excess_returns > 0).mean()
        excess_calmar = alpha / abs(excess_max_dd) if excess_max_dd != 0 else 0
        
        return {
            "total_return": total_return,
            "annualized_return": ann_return,
            "annualized_volatility": ann_vol,
            "sharpe_ratio": sharpe,
            "max_drawdown": max_dd,
            "calmar_ratio": calmar,
            "benchmark_return": benchmark_ann,
            "benchmark_volatility": benchmark_vol,
            "alpha": alpha,
            "tracking_error": tracking_error,
            "information_ratio": info_ratio,
            "win_rate": win_rate,
            "total_trades": len(self.trade_log),
            "trading_days": n_days,
            "start_date": str(self.nav_series.index[0].date()),
            "end_date": str(self.nav_series.index[-1].date()),
            # Excess return metrics
            "excess_max_drawdown": excess_max_dd,
            "excess_win_rate": excess_win_rate,
            "excess_calmar": excess_calmar,
        }
    
    def get_metrics_dataframe(self) -> pd.DataFrame:
        """Get metrics as a formatted DataFrame for display."""
        m = self.compute_metrics()
        
        data = [
            ("Period", f"{m['start_date']} to {m['end_date']}"),
            ("Trading Days", f"{m['trading_days']:.0f}"),
            ("", ""),
            ("Strategy Performance", ""),
            ("Total Return", f"{m['total_return']:.2%}"),
            ("Annualized Return", f"{m['annualized_return']:.2%}"),
            ("Annualized Volatility", f"{m['annualized_volatility']:.2%}"),
            ("Sharpe Ratio*", f"{m['sharpe_ratio']:.2f}"),
            ("Max Drawdown", f"{m['max_drawdown']:.2%}"),
            ("Calmar Ratio", f"{m['calmar_ratio']:.2f}"),
            ("Win Rate (Daily)", f"{m['win_rate']:.2%}"),
            ("", ""),
            ("Benchmark Performance", ""),
            ("Ann. Return", f"{m['benchmark_return']:.2%}"),
            ("Ann. Volatility", f"{m['benchmark_volatility']:.2%}"),
            ("", ""),
            ("Excess Performance (Alpha)", ""),
            ("Alpha (Ann.)", f"{m['alpha']:.2%}"),
            ("Tracking Error", f"{m['tracking_error']:.2%}"),
            ("Information Ratio**", f"{m['information_ratio']:.2f}"),
            ("Excess Max Drawdown", f"{m['excess_max_drawdown']:.2%}"),
            ("Excess Win Rate", f"{m['excess_win_rate']:.2%}"),
            ("Excess Calmar", f"{m['excess_calmar']:.2f}"),
            ("", ""),
            ("Trading", ""),
            ("Total Trades", f"{m['total_trades']:.0f}"),
            ("", ""),
            ("* Sharpe = (Ann.Return - Rf) / Vol", f"Rf = {self.risk_free_rate:.1%}"),
            ("** IR = Alpha / Tracking Error", ""),
        ]
        
        return pd.DataFrame(data, columns=["Metric", "Value"])
    
    def plot_comprehensive_report(
        self,
        figsize: tuple = (16, 12),
        dpi: int = 150,
    ) -> Figure:
        """
        Create a comprehensive report figure with:
        - NAV comparison with drawdown overlay
        - Excess returns
        - Monthly returns heatmap
        - Metrics table
        """
        fig = plt.figure(figsize=figsize, dpi=dpi)
        gs = GridSpec(3, 2, figure=fig, height_ratios=[2, 1, 1.5], hspace=0.3, wspace=0.25)
        
        # Top left: NAV comparison with drawdown
        ax1 = fig.add_subplot(gs[0, 0])
        self._plot_nav_with_drawdown(ax1)
        
        # Top right: Metrics table
        ax2 = fig.add_subplot(gs[0, 1])
        self._plot_metrics_table(ax2)
        
        # Middle left: Excess NAV
        ax3 = fig.add_subplot(gs[1, 0])
        self._plot_excess_nav(ax3)
        
        # Middle right: Yearly returns
        ax4 = fig.add_subplot(gs[1, 1])
        self._plot_yearly_returns(ax4)
        
        # Bottom: Monthly returns heatmap
        ax5 = fig.add_subplot(gs[2, :])
        self._plot_monthly_heatmap(ax5)
        
        fig.suptitle(
            f"{self.strategy_name} vs {self.benchmark_name} - Backtest Report",
            fontsize=14,
            fontweight='bold',
            y=0.98
        )
        
        return fig
    
    def _plot_nav_with_drawdown(self, ax: plt.Axes) -> None:
        """Plot NAV curves with drawdown shading."""
        ax.plot(self.nav_series.index, self.nav_series.values,
                label=self.strategy_name, linewidth=1.5, color='#2E86AB')
        ax.plot(self.benchmark_nav.index, self.benchmark_nav.values,
                label=self.benchmark_name, linewidth=1.5, color='#A23B72', alpha=0.7)
        
        # Drawdown shading
        cummax = self.nav_series.cummax()
        drawdown = (self.nav_series - cummax) / cummax
        
        ax2 = ax.twinx()
        ax2.fill_between(drawdown.index, drawdown.values, 0,
                         alpha=0.2, color='red', label='Drawdown')
        ax2.set_ylabel('Drawdown', color='red')
        ax2.tick_params(axis='y', labelcolor='red')
        ax2.set_ylim(drawdown.min() * 1.5, 0.1)
        
        ax.set_xlabel('Date')
        ax.set_ylabel('NAV')
        ax.set_title('NAV Comparison with Drawdown')
        ax.legend(loc='upper left')
        ax.grid(True, alpha=0.3)
    
    def _plot_metrics_table(self, ax: plt.Axes) -> None:
        """Plot metrics as a table."""
        ax.axis('off')
        
        m = self.compute_metrics()
        
        cell_text = [
            ["── Strategy ──", ""],
            ["Total Return", f"{m['total_return']:.2%}"],
            ["Ann. Return", f"{m['annualized_return']:.2%}"],
            ["Ann. Volatility", f"{m['annualized_volatility']:.2%}"],
            ["Sharpe*", f"{m['sharpe_ratio']:.2f}"],
            ["Max Drawdown", f"{m['max_drawdown']:.2%}"],
            ["── Benchmark ──", ""],
            ["Benchmark Return", f"{m['benchmark_return']:.2%}"],
            ["── Excess (Alpha) ──", ""],
            ["Alpha (Ann.)", f"{m['alpha']:.2%}"],
            ["Tracking Error", f"{m['tracking_error']:.2%}"],
            ["Info Ratio**", f"{m['information_ratio']:.2f}"],
            ["Excess Max DD", f"{m['excess_max_drawdown']:.2%}"],
            ["Excess Win Rate", f"{m['excess_win_rate']:.2%}"],
            ["── Trading ──", ""],
            ["Total Trades", f"{m['total_trades']:.0f}"],
        ]
        
        table = ax.table(
            cellText=cell_text,
            colLabels=["Metric", "Value"],
            loc='center',
            cellLoc='left',
            colWidths=[0.5, 0.3],
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1.2, 1.5)
        
        # Style header
        for j in range(2):
            table[(0, j)].set_facecolor('#4472C4')
            table[(0, j)].set_text_props(color='white', fontweight='bold')
        
        # Style section headers (rows with "──")
        for i in range(1, len(cell_text) + 1):
            if cell_text[i-1][0].startswith("──"):
                table[(i, 0)].set_text_props(fontweight='bold', color='#2E86AB')
                table[(i, 1)].set_text_props(fontweight='bold')
        
        ax.set_title('Performance Metrics', fontweight='bold', pad=20)
        
        # Add formula notes below table
        rf = self.risk_free_rate
        ax.text(0.5, -0.05, 
                f"* Sharpe = (Ann.Return - Rf) / Ann.Vol, Rf={rf:.1%}\n"
                f"** Info Ratio = Alpha / Tracking Error",
                transform=ax.transAxes, fontsize=8, ha='center', va='top',
                style='italic', color='gray')
    
    def _plot_excess_nav(self, ax: plt.Axes) -> None:
        """Plot excess NAV (strategy / benchmark) with drawdown overlay."""
        excess_nav = self.nav_series / self.benchmark_nav
        
        ax.plot(excess_nav.index, excess_nav.values,
                linewidth=1.5, color='#28A745', label='Excess NAV')
        ax.axhline(y=1.0, color='gray', linestyle='--', alpha=0.5)
        ax.fill_between(
            excess_nav.index,
            1.0,
            excess_nav.values,
            where=(excess_nav.values >= 1),
            alpha=0.3,
            color='green'
        )
        ax.fill_between(
            excess_nav.index,
            1.0,
            excess_nav.values,
            where=(excess_nav.values < 1),
            alpha=0.3,
            color='red'
        )
        
        # Add excess drawdown on secondary axis
        excess_cummax = excess_nav.cummax()
        excess_dd = (excess_nav - excess_cummax) / excess_cummax
        
        ax2 = ax.twinx()
        ax2.fill_between(excess_dd.index, excess_dd.values, 0,
                         alpha=0.2, color='orange', label='Excess DD')
        ax2.set_ylabel('Excess Drawdown', color='orange')
        ax2.tick_params(axis='y', labelcolor='orange')
        ax2.set_ylim(excess_dd.min() * 1.5, 0.1)
        
        ax.set_xlabel('Date')
        ax.set_ylabel('Excess NAV')
        ax.set_title(f'Excess Return with Drawdown (Max DD: {excess_dd.min():.2%})')
        ax.legend(loc='upper left')
        ax.grid(True, alpha=0.3)
    
    def _plot_yearly_returns(self, ax: plt.Axes) -> None:
        """Plot yearly returns comparison."""
        strategy_returns = self.nav_series.pct_change().dropna()
        benchmark_returns = self.benchmark_nav.pct_change().dropna()
        
        yearly_strat = (1 + strategy_returns).resample('YE').prod() - 1
        yearly_bench = (1 + benchmark_returns).resample('YE').prod() - 1
        
        years = yearly_strat.index.year
        x = np.arange(len(years))
        width = 0.35
        
        ax.bar(x - width/2, yearly_strat.values, width, label=self.strategy_name, color='#2E86AB')
        ax.bar(x + width/2, yearly_bench.values, width, label=self.benchmark_name, color='#A23B72', alpha=0.7)
        
        ax.set_xticks(x)
        ax.set_xticklabels(years)
        ax.set_xlabel('Year')
        ax.set_ylabel('Return')
        ax.set_title('Yearly Returns Comparison')
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        ax.axhline(y=0, color='black', linewidth=0.5)
        
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))
    
    def _plot_monthly_heatmap(self, ax: plt.Axes) -> None:
        """Plot monthly returns heatmap."""
        monthly_nav = self.nav_series.resample('ME').last()
        monthly_returns = monthly_nav.pct_change().dropna()
        
        monthly_returns.index = pd.to_datetime(monthly_returns.index)
        df = pd.DataFrame({
            'year': monthly_returns.index.year,
            'month': monthly_returns.index.month,
            'return': monthly_returns.values
        })
        pivot = df.pivot(index='year', columns='month', values='return')
        
        im = ax.imshow(pivot.values, cmap='RdYlGn', aspect='auto', vmin=-0.1, vmax=0.1)
        
        ax.set_xticks(np.arange(12))
        ax.set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
        ax.set_yticks(np.arange(len(pivot.index)))
        ax.set_yticklabels(pivot.index)
        
        for i in range(len(pivot.index)):
            for j in range(12):
                if j < pivot.shape[1]:
                    val = pivot.iloc[i, j]
                    if not np.isnan(val):
                        ax.text(j, i, f"{val:.1%}", ha="center", va="center",
                               color="black" if abs(val) < 0.05 else "white",
                               fontsize=7)
        
        ax.set_title(f'{self.strategy_name} Monthly Returns')
        plt.colorbar(im, ax=ax, label='Return', shrink=0.6)
    
    def generate_report(self) -> str:
        """Generate text report of performance metrics."""
        m = self.compute_metrics()
        
        lines = [
            "=" * 60,
            f"PERFORMANCE REPORT: {self.strategy_name}",
            "=" * 60,
            f"Period: {m['start_date']} to {m['end_date']}",
            "",
            "Strategy Performance:",
            "-" * 40,
            f"  Total Return:          {m['total_return']:.2%}",
            f"  Annualized Return:     {m['annualized_return']:.2%}",
            f"  Annualized Volatility: {m['annualized_volatility']:.2%}",
            f"  Sharpe Ratio:          {m['sharpe_ratio']:.2f}",
            f"  Max Drawdown:          {m['max_drawdown']:.2%}",
            f"  Calmar Ratio:          {m['calmar_ratio']:.2f}",
            f"  Win Rate:              {m['win_rate']:.2%}",
            "",
            f"Benchmark Performance ({self.benchmark_name}):",
            "-" * 40,
            f"  Annualized Return:     {m['benchmark_return']:.2%}",
            f"  Annualized Volatility: {m['benchmark_volatility']:.2%}",
            "",
            "Excess Performance (Alpha):",
            "-" * 40,
            f"  Alpha (Ann.):          {m['alpha']:.2%}",
            f"  Tracking Error:        {m['tracking_error']:.2%}",
            f"  Information Ratio:     {m['information_ratio']:.2f}",
            f"  Excess Max Drawdown:   {m['excess_max_drawdown']:.2%}",
            f"  Excess Win Rate:       {m['excess_win_rate']:.2%}",
            f"  Excess Calmar:         {m['excess_calmar']:.2f}",
            "",
            "Trading Statistics:",
            "-" * 40,
            f"  Trading Days:          {m['trading_days']:.0f}",
            f"  Total Trades:          {m['total_trades']:.0f}",
            "",
            "=" * 60,
        ]
        
        return "\n".join(lines)
    
    def export_trade_log(self, output_path: str | Path) -> None:
        """Export trade log to CSV file."""
        if not self.trade_log:
            return
        
        records = [
            {
                "trade_date": t.trade_date,
                "ts_code": t.ts_code,
                "direction": t.direction,
                "volume": t.volume,
                "price": t.price,
                "amount": t.amount,
                "commission": t.commission,
                "realized_pnl": t.realized_pnl,
                "reason": t.reason,
            }
            for t in self.trade_log
        ]
        
        df = pd.DataFrame(records)
        df.to_csv(output_path, index=False)
    
    def export_nav_series(self, output_path: str | Path) -> None:
        """Export NAV series to CSV file."""
        df = pd.DataFrame({
            "date": self.nav_series.index,
            "strategy_nav": self.nav_series.values,
            "benchmark_nav": self.benchmark_nav.values,
            "excess_nav": (self.nav_series / self.benchmark_nav).values,
        })
        df.to_csv(output_path, index=False)
    
    def save_all(
        self,
        output_dir: str | Path,
        run_name: str = "backtest",
        dpi: int = 150,
        fmt: str = "png",
    ) -> None:
        """
        Save comprehensive report, trade log, and NAV series.
        
        Creates directory structure:
            output_dir/
                run_name/
                    report.png
                    trade_log.csv
                    nav_series.csv
                    metrics.csv
                    report.txt
        """
        output_dir = Path(output_dir) / run_name
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save comprehensive figure
        fig = self.plot_comprehensive_report(dpi=dpi)
        fig.savefig(output_dir / f"report.{fmt}", dpi=dpi, bbox_inches='tight')
        plt.close(fig)
        
        # Save trade log
        if self.trade_log:
            self.export_trade_log(output_dir / "trade_log.csv")
        
        # Save NAV series
        self.export_nav_series(output_dir / "nav_series.csv")
        
        # Save metrics
        metrics_df = self.get_metrics_dataframe()
        metrics_df.to_csv(output_dir / "metrics.csv", index=False)
        
        # Save text report
        with open(output_dir / "report.txt", "w") as f:
            f.write(self.generate_report())
    
    # Legacy methods for backward compatibility
    def plot_nav_comparison(self, figsize=(12, 6), title=None) -> Figure:
        fig, ax = plt.subplots(figsize=figsize)
        ax.plot(self.nav_series.index, self.nav_series.values,
                label=self.strategy_name, linewidth=1.5)
        ax.plot(self.benchmark_nav.index, self.benchmark_nav.values,
                label=self.benchmark_name, linewidth=1.5, alpha=0.7)
        ax.set_xlabel('Date')
        ax.set_ylabel('NAV')
        ax.set_title(title or f'{self.strategy_name} vs {self.benchmark_name}')
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        return fig
    
    def plot_drawdown(self, figsize=(12, 4), title=None) -> Figure:
        fig, ax = plt.subplots(figsize=figsize)
        cummax = self.nav_series.cummax()
        drawdown = (self.nav_series - cummax) / cummax
        ax.fill_between(drawdown.index, drawdown.values, 0, alpha=0.5, color='red')
        ax.set_xlabel('Date')
        ax.set_ylabel('Drawdown')
        ax.set_title(title or f'{self.strategy_name} Drawdown')
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        return fig
    
    def save_plots(self, output_dir: str, prefix: str = "backtest") -> None:
        """Legacy method - use save_all instead."""
        self.save_all(output_dir, run_name=prefix)
