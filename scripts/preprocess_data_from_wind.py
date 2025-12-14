"""
Data preprocessing script: convert Wind raw data to processed parquet files.
Wind data lacks delist_date and last_ddate, which are fetched from tushare info.csv.
"""
import polars as pl
from pathlib import Path
from loguru import logger
from datetime import datetime


NEW_RAW_DATA_PATH = Path("/root/sw1/new_raw_data")
TUSHARE_INFO_PATH = Path("/root/sw1/raw_data/info/info.csv")
PROCESSED_DATA_PATH = Path("/root/sw1/processed_data")

INDEX_MAPPING = {
    "000905.SH": ("CSI500", "IC"),
    "000852.SH": ("CSI1000", "IM"),
    "000300.SH": ("CSI300", "IF"),
}


def ensure_dirs():
    """Create processed data directories."""
    for subdir in ["futures", "index", "contracts", "margin"]:
        (PROCESSED_DATA_PATH / subdir).mkdir(parents=True, exist_ok=True)
    logger.info(f"Created directory structure at {PROCESSED_DATA_PATH}")


def load_tushare_delist_info() -> pl.DataFrame:
    """Load delist_date and last_ddate from tushare info.csv."""
    df = pl.read_csv(TUSHARE_INFO_PATH)
    
    df = df.filter(
        pl.col("delist_date").is_not_null() &
        pl.col("last_ddate").is_not_null() &
        pl.col("multiplier").is_not_null()
    )
    
    df = df.with_columns([
        pl.col("ts_code").str.replace(".CFX", ".CFE").alias("ts_code"),
    ])
    
    df = df.select([
        "ts_code",
        pl.col("delist_date").cast(pl.Utf8),
        pl.col("last_ddate").cast(pl.Utf8),
    ])
    
    return df


def process_futures_daily():
    """Process futures daily bars from Wind data."""
    raw_path = NEW_RAW_DATA_PATH / "all_futures_daily_full.csv"
    
    df = pl.read_csv(raw_path)
    
    df = df.filter(
        ~pl.col("S_INFO_CODE").str.contains("IFM") &
        (pl.col("S_INFO_CODE").is_in(["IC", "IF", "IM"]))
    )
    
    df = df.with_columns(
        pl.col("TRADE_DT").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d").alias("trade_date")
    )
    
    df = df.sort(["S_INFO_WINDCODE", "trade_date"])
    
    df = df.select([
        pl.col("S_INFO_WINDCODE").alias("ts_code"),
        "trade_date",
        pl.col("S_DQ_CLOSE").shift(1).over("S_INFO_WINDCODE").alias("pre_close"),
        pl.col("PRE_SETTLE").alias("pre_settle"),
        pl.col("S_DQ_OPEN").alias("open"),
        pl.col("S_DQ_HIGH").alias("high"),
        pl.col("S_DQ_LOW").alias("low"),
        pl.col("S_DQ_CLOSE").alias("close"),
        pl.col("SETTLE_PRICE").alias("settle"),
        pl.col("S_DQ_VOLUME").alias("volume"),
        pl.col("TURNOVER_VALUE").alias("amount"),
        pl.col("OPEN_INTEREST").alias("open_interest"),
        (pl.col("OPEN_INTEREST") - pl.col("OPEN_INTEREST").shift(1).over("S_INFO_WINDCODE")).alias("oi_change"),
    ])
    
    df = df.with_columns(
        pl.col("ts_code").str.extract(r"^([A-Z]+)", 1).alias("fut_code")
    )
    
    for fut_code in ["IC", "IM", "IF"]:
        fut_df = df.filter(pl.col("fut_code") == fut_code).drop("fut_code")
        fut_df = fut_df.sort(["ts_code", "trade_date"])
        
        output_path = PROCESSED_DATA_PATH / "futures" / f"{fut_code}_daily.parquet"
        fut_df.write_parquet(output_path)
        logger.info(f"Processed {fut_code} futures: {len(fut_df)} rows -> {output_path}")


def process_index_daily():
    """Process index daily bars from Wind data."""
    raw_path = NEW_RAW_DATA_PATH / "all_index_daily.csv"
    
    df = pl.read_csv(raw_path)
    
    df = df.with_columns(
        pl.col("TRADE_DT").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d").alias("trade_date")
    )
    
    df = df.select([
        pl.col("S_INFO_WINDCODE").alias("index_code"),
        "trade_date",
        pl.col("S_DQ_OPEN").alias("open"),
        pl.col("S_DQ_HIGH").alias("high"),
        pl.col("S_DQ_LOW").alias("low"),
        pl.col("S_DQ_CLOSE").alias("close"),
    ])
    
    for index_code, (index_name, _) in INDEX_MAPPING.items():
        index_df = df.filter(pl.col("index_code") == index_code).sort("trade_date")
        
        output_path = PROCESSED_DATA_PATH / "index" / f"{index_name}_daily.parquet"
        index_df.write_parquet(output_path)
        logger.info(f"Processed {index_name} index: {len(index_df)} rows -> {output_path}")


def process_contract_info():
    """Process contract info from Wind data, merging delist_date from tushare."""
    raw_path = NEW_RAW_DATA_PATH / "all_futures_info.csv"
    
    df = pl.read_csv(raw_path)
    
    df = df.filter(
        ~pl.col("S_INFO_CODE").str.contains("IFM") &
        (pl.col("S_INFO_CODE").is_in(["IC", "IF", "IM"]))
    )
    
    df = df.with_columns([
        pl.col("S_INFO_WINDCODE").alias("ts_code"),
        pl.col("S_INFO_CODE").alias("fut_code"),
        pl.col("S_INFO_WINDCODE").str.extract(r"([A-Z]+)(\d+)", 2).alias("symbol"),
        pl.col("S_INFO_NAME").alias("name"),
        pl.col("MULTIPLIER").cast(pl.Float64).alias("multiplier"),
    ])
    
    df = df.with_columns(
        pl.col("LIST_DATE").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d").alias("list_date")
    )
    
    tushare_info = load_tushare_delist_info()
    
    df = df.join(tushare_info, on="ts_code", how="left")
    
    df = df.filter(pl.col("delist_date").is_not_null())
    
    df = df.with_columns([
        pl.col("delist_date").str.strptime(pl.Date, "%Y%m%d").alias("delist_date"),
        pl.col("last_ddate").str.strptime(pl.Date, "%Y%m%d").alias("last_ddate"),
    ])
    
    df = df.select([
        "ts_code",
        pl.col("symbol").alias("symbol"),
        "fut_code",
        "multiplier",
        "list_date",
        "delist_date",
        "last_ddate",
        "name",
    ])
    
    for fut_code in ["IC", "IM", "IF"]:
        fut_df = df.filter(pl.col("fut_code") == fut_code).sort("list_date")
        
        output_path = PROCESSED_DATA_PATH / "contracts" / f"{fut_code}_info.parquet"
        fut_df.write_parquet(output_path)
        logger.info(f"Processed {fut_code} contracts: {len(fut_df)} rows -> {output_path}")


def process_margin_ratio():
    """Process margin ratio history from Wind data."""
    raw_path = NEW_RAW_DATA_PATH / "all_futures_margin.csv"
    
    df = pl.read_csv(raw_path)
    
    df = df.with_columns(
        pl.col("TRADE_DT").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d").alias("trade_date")
    )
    
    df = df.with_columns(
        pl.col("S_INFO_WINDCODE").str.extract(r"^([A-Z]+)", 1).alias("fut_code")
    )
    
    df = df.filter(pl.col("fut_code").is_in(["IC", "IF", "IM"]))
    
    df = df.select([
        "fut_code",
        "trade_date",
        (pl.col("LONG_MARGIN") / 100.0).alias("long_margin_ratio"),
        (pl.col("SHORT_MARGIN") / 100.0).alias("short_margin_ratio"),
    ])
    
    df = df.group_by(["fut_code", "trade_date"]).agg([
        pl.col("long_margin_ratio").first(),
        pl.col("short_margin_ratio").first(),
    ]).sort(["fut_code", "trade_date"])
    
    output_path = PROCESSED_DATA_PATH / "margin" / "margin_ratio.parquet"
    df.write_parquet(output_path)
    logger.info(f"Processed margin ratio: {len(df)} rows -> {output_path}")


def validate_data():
    """Validate processed data."""
    logger.info("Validating processed data...")
    
    for fut_code in ["IC", "IM", "IF"]:
        path = PROCESSED_DATA_PATH / "futures" / f"{fut_code}_daily.parquet"
        if path.exists():
            df = pl.read_parquet(path)
            contracts = df["ts_code"].unique().len()
            date_range = f"{df['trade_date'].min()} to {df['trade_date'].max()}"
            logger.info(f"  {fut_code} futures: {contracts} contracts, {len(df)} bars, {date_range}")
    
    for index_code, (index_name, _) in INDEX_MAPPING.items():
        path = PROCESSED_DATA_PATH / "index" / f"{index_name}_daily.parquet"
        if path.exists():
            df = pl.read_parquet(path)
            date_range = f"{df['trade_date'].min()} to {df['trade_date'].max()}"
            logger.info(f"  {index_name} index: {len(df)} bars, {date_range}")
    
    for fut_code in ["IC", "IM", "IF"]:
        path = PROCESSED_DATA_PATH / "contracts" / f"{fut_code}_info.parquet"
        if path.exists():
            df = pl.read_parquet(path)
            logger.info(f"  {fut_code} contracts: {len(df)} contracts")
    
    path = PROCESSED_DATA_PATH / "margin" / "margin_ratio.parquet"
    if path.exists():
        df = pl.read_parquet(path)
        logger.info(f"  Margin ratio: {len(df)} records")


def main():
    logger.info("Starting Wind data preprocessing...")
    
    ensure_dirs()
    process_futures_daily()
    process_index_daily()
    process_contract_info()
    process_margin_ratio()
    validate_data()
    
    logger.info("Wind data preprocessing completed!")


if __name__ == "__main__":
    main()
