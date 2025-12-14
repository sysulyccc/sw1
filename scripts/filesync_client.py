"""
FileSyncClient: OOP wrapper for accessing the school Oracle database (FILESYNC schema).
Combines functionality from get_data.py and search.py.
"""
import os
from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass
from loguru import logger

import pandas as pd

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str = '219.223.208.52'
    port: str = '1521'
    service: str = 'orcl'
    user: str = 'Your username'
    password: str = 'Your password'
    oracle_client_path: str = 'Your oracle path'


class FileSyncClient:
    """
    Client for accessing futures and index data from the FILESYNC Oracle database.
    
    Usage:
        client = FileSyncClient(config)
        client.connect()
        
        # Search for tables
        tables = client.search_tables(pattern='FUTURE%PRICE%')
        
        # Download data
        client.download_index_daily(output_path='all_index_daily.csv')
        client.download_futures_info(output_path='all_futures_info.csv')
        client.download_futures_daily(output_path='all_futures_daily_full.csv')
        client.download_margin_ratio(output_path='all_futures_margin.csv')
        client.download_price_limits(output_path='all_futures_limits.csv')
        client.download_main_contracts(output_path='main_contracts.csv')
        
        client.close()
    """
    
    INDEX_CODES = ('000300.SH', '000905.SH', '000852.SH')
    FUTURES_FILTER = """
        (S_INFO_WINDCODE LIKE 'IF%.CFE' OR 
         S_INFO_WINDCODE LIKE 'IC%.CFE' OR 
         S_INFO_WINDCODE LIKE 'IM%.CFE')
    """
    
    def __init__(
        self,
        config: Optional[DatabaseConfig] = None,
        start_date: str = '20100101',
        end_date: str = '20251231',
    ):
        """
        Initialize the FileSyncClient.
        
        Args:
            config: Database connection configuration
            start_date: Start date for data download (YYYYMMDD)
            end_date: End date for data download (YYYYMMDD)
        """
        self.config = config or DatabaseConfig()
        self.start_date = start_date
        self.end_date = end_date
        self._conn = None
        self._cx_oracle = None
    
    def _init_oracle_client(self):
        """Initialize Oracle client library."""
        import cx_Oracle
        self._cx_oracle = cx_Oracle
        try:
            cx_Oracle.init_oracle_client(lib_dir=self.config.oracle_client_path)
        except Exception:
            pass
    
    def connect(self):
        """Establish database connection."""
        self._init_oracle_client()
        dsn = self._cx_oracle.makedsn(
            self.config.host,
            self.config.port,
            service_name=self.config.service
        )
        self._conn = self._cx_oracle.connect(
            user=self.config.user,
            password=self.config.password,
            dsn=dsn
        )
        logger.info("Database connection established")
    
    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Database connection closed")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def _execute_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return DataFrame."""
        if self._conn is None:
            raise RuntimeError("Not connected to database. Call connect() first.")
        return pd.read_sql(sql, self._conn)
    
    def search_tables(
        self,
        pattern: str = '%FUTURE%',
        schema: str = 'FILESYNC'
    ) -> pd.DataFrame:
        """
        Search for tables in the database matching a pattern.
        
        Args:
            pattern: SQL LIKE pattern for table name
            schema: Schema to search in
            
        Returns:
            DataFrame with OWNER and TABLE_NAME columns
        """
        sql = f"""
            SELECT OWNER, TABLE_NAME 
            FROM ALL_TABLES 
            WHERE TABLE_NAME LIKE '{pattern}'
              AND OWNER = '{schema}'
            ORDER BY TABLE_NAME
        """
        df = self._execute_query(sql)
        logger.info(f"Found {len(df)} tables matching pattern '{pattern}'")
        return df
    
    def search_futures_tables(self) -> pd.DataFrame:
        """Search for futures-related tables (EOD prices)."""
        sql = """
            SELECT OWNER, TABLE_NAME 
            FROM ALL_TABLES 
            WHERE (TABLE_NAME LIKE '%FUTURE%PRICE%' 
               OR TABLE_NAME LIKE '%FUTURE%EOD%')
              AND OWNER = 'FILESYNC'
            ORDER BY TABLE_NAME
        """
        return self._execute_query(sql)
    
    def download_index_daily(self, output_path: Optional[str] = None) -> pd.DataFrame:
        """
        Download spot index daily data (AINDEXEODPRICES).
        
        Args:
            output_path: Optional path to save CSV file
            
        Returns:
            DataFrame with index daily data
        """
        index_codes_sql = str(self.INDEX_CODES)
        sql = f"""
            SELECT S_INFO_WINDCODE, TRADE_DT, 
                   S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE
            FROM FILESYNC.AINDEXEODPRICES
            WHERE S_INFO_WINDCODE IN {index_codes_sql}
              AND TRADE_DT BETWEEN '{self.start_date}' AND '{self.end_date}'
            ORDER BY TRADE_DT
        """
        logger.info("Downloading spot index data...")
        df = self._execute_query(sql)
        
        if output_path:
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"Saved index data to {output_path}")
        
        return df
    
    def download_futures_info(self, output_path: Optional[str] = None) -> pd.DataFrame:
        """
        Download futures contract static info (CFUTURESCONTPRO).
        
        Args:
            output_path: Optional path to save CSV file
            
        Returns:
            DataFrame with contract info
        """
        sql = f"""
            SELECT S_INFO_WINDCODE, S_INFO_CODE, S_INFO_NAME, 
                   S_INFO_LISTDATE, S_INFO_DELISTDATE, 
                   S_INFO_LTDATED, S_INFO_CEMULTIPLIER, S_INFO_MFPRICE
            FROM FILESYNC.CFUTURESCONTPRO
            WHERE {self.FUTURES_FILTER}
        """
        logger.info("Downloading futures contract info...")
        df = self._execute_query(sql)
        
        df.rename(columns={
            'S_INFO_LTDATED': 'LAST_TRADE_DATE',
            'S_INFO_CEMULTIPLIER': 'MULTIPLIER',
            'S_INFO_MFPRICE': 'TICK_SIZE',
            'S_INFO_LISTDATE': 'LIST_DATE',
            'S_INFO_DELISTDATE': 'DELIST_DATE'
        }, inplace=True)
        
        if output_path:
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"Saved futures info to {output_path}")
        
        return df
    
    def download_futures_daily(self, output_path: Optional[str] = None) -> pd.DataFrame:
        """
        Download futures daily prices with contract info merged (CINDEXFUTURESEODPRICES).
        
        Args:
            output_path: Optional path to save CSV file
            
        Returns:
            DataFrame with futures daily data
        """
        sql_daily = f"""
            SELECT S_INFO_WINDCODE, TRADE_DT, 
                   S_DQ_PRESETTLE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE, S_DQ_SETTLE,
                   S_DQ_CHANGE, S_DQ_VOLUME, S_DQ_AMOUNT, S_DQ_OI
            FROM FILESYNC.CINDEXFUTURESEODPRICES
            WHERE {self.FUTURES_FILTER}
              AND TRADE_DT BETWEEN '{self.start_date}' AND '{self.end_date}'
        """
        logger.info("Downloading futures daily data...")
        df_daily = self._execute_query(sql_daily)
        
        df_daily.rename(columns={
            'S_DQ_PRESETTLE': 'PRE_SETTLE',
            'S_DQ_SETTLE': 'SETTLE_PRICE',
            'S_DQ_AMOUNT': 'TURNOVER_VALUE',
            'S_DQ_OI': 'OPEN_INTEREST'
        }, inplace=True)
        
        df_info = self.download_futures_info()
        df_merged = pd.merge(df_daily, df_info, on='S_INFO_WINDCODE', how='inner')
        
        if output_path:
            df_merged.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"Saved futures daily data to {output_path}")
        
        return df_merged
    
    def download_main_contracts(self, output_path: Optional[str] = None) -> pd.DataFrame:
        """
        Download official main contract mapping (CFUTURESCONTRACTMAPPING).
        
        Args:
            output_path: Optional path to save CSV file
            
        Returns:
            DataFrame with main contract mapping (pivoted by variety)
        """
        sql = """
            SELECT S_INFO_WINDCODE, FS_MAPPING_WINDCODE, STARTDATE, ENDDATE
            FROM FILESYNC.CFUTURESCONTRACTMAPPING
            WHERE S_INFO_WINDCODE IN ('IF00.CFE', 'IC00.CFE', 'IM00.CFE')
            ORDER BY S_INFO_WINDCODE, STARTDATE
        """
        logger.info("Downloading main contract mapping...")
        df_mapping = self._execute_query(sql)
        
        all_dates_data = []
        for _, row in df_mapping.iterrows():
            variety = row['S_INFO_WINDCODE'][:2]
            real_code = row['FS_MAPPING_WINDCODE']
            dates = pd.date_range(start=row['STARTDATE'], end=row['ENDDATE'], freq='D')
            temp_df = pd.DataFrame({
                'TRADE_DT': dates.strftime('%Y%m%d'),
                'VARIETY': variety,
                'MAIN_CODE': real_code
            })
            all_dates_data.append(temp_df)
        
        if all_dates_data:
            df_expanded = pd.concat(all_dates_data)
            df_expanded = df_expanded[
                (df_expanded['TRADE_DT'] >= self.start_date) &
                (df_expanded['TRADE_DT'] <= self.end_date)
            ]
            df_main = df_expanded.pivot_table(
                index='TRADE_DT',
                columns='VARIETY',
                values='MAIN_CODE',
                aggfunc='first'
            )
            df_main.reset_index(inplace=True)
            
            if output_path:
                df_main.to_csv(output_path, index=False, encoding='utf-8-sig')
                logger.info(f"Saved main contracts to {output_path}")
            
            return df_main
        
        return pd.DataFrame()
    
    def download_margin_ratio(self, output_path: Optional[str] = None) -> pd.DataFrame:
        """
        Download margin ratios (CFUTURESMARGINRATIO).
        
        Args:
            output_path: Optional path to save CSV file
            
        Returns:
            DataFrame with margin ratio data
        """
        sql = f"""
            SELECT S_INFO_WINDCODE, TRADE_DT, MARGINRATIO, MARGINRATIO_SHORT
            FROM FILESYNC.CFUTURESMARGINRATIO
            WHERE {self.FUTURES_FILTER} 
              AND TRADE_DT BETWEEN '{self.start_date}' AND '{self.end_date}'
        """
        logger.info("Downloading margin ratios...")
        df = self._execute_query(sql)
        
        df.rename(columns={
            'MARGINRATIO': 'LONG_MARGIN',
            'MARGINRATIO_SHORT': 'SHORT_MARGIN'
        }, inplace=True)
        
        if output_path:
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"Saved margin ratios to {output_path}")
        
        return df
    
    def download_price_limits(self, output_path: Optional[str] = None) -> pd.DataFrame:
        """
        Download price limit ratios (CFUTURESPRICECHANGELIMIT).
        
        Args:
            output_path: Optional path to save CSV file
            
        Returns:
            DataFrame with price limit data
        """
        sql = f"""
            SELECT S_INFO_WINDCODE, PCT_CHG_LIMIT, CHANGE_DT
            FROM FILESYNC.CFUTURESPRICECHANGELIMIT
            WHERE {self.FUTURES_FILTER}
        """
        logger.info("Downloading price limits...")
        df = self._execute_query(sql)
        
        df.rename(columns={
            'PCT_CHG_LIMIT': 'LIMIT_PCT',
            'CHANGE_DT': 'EFFECTIVE_DATE'
        }, inplace=True)
        
        if output_path:
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"Saved price limits to {output_path}")
        
        return df
    
    def download_all(self, output_dir: str = '.'):
        """
        Download all data files to the specified directory.
        
        Args:
            output_dir: Directory to save output files
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Downloading all data to {output_path}")
        
        self.download_index_daily(output_path / 'all_index_daily.csv')
        self.download_futures_info(output_path / 'all_futures_info.csv')
        self.download_futures_daily(output_path / 'all_futures_daily_full.csv')
        self.download_main_contracts(output_path / 'main_contracts.csv')
        self.download_margin_ratio(output_path / 'all_futures_margin.csv')
        self.download_price_limits(output_path / 'all_futures_limits.csv')
        
        logger.info("All data files downloaded successfully")


def main():
    """Example usage of FileSyncClient."""
    config = DatabaseConfig(
        host='219.223.208.52',
        port='1521',
        service='orcl',
        user='Your username',
        password='Your password',
        oracle_client_path='Your oracle path',
    )
    
    with FileSyncClient(config) as client:
        tables = client.search_futures_tables()
        print("Found tables:")
        print(tables)
        
        # client.download_all(output_dir='./new_raw_data')


if __name__ == '__main__':
    main()
