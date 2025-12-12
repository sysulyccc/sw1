import os
import cx_Oracle
import pandas as pd
import numpy as np

# ================= Configuration =================
# 1. Force character set (avoid garbled Chinese characters)
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

# 2. Database configuration
# Note: change this path to your actual local Oracle client directory
lib_path = "Your oracle path"
DB_HOST = '219.223.208.52'
DB_PORT = '1521'
DB_SERVICE = 'orcl'
DB_USER = 'Your username'
DB_PASS = 'Your password'

# 3. Time range and filter (2010–2025 full sample)
START_DATE = '20100101'
END_DATE = '20251231'
INDEX_CODES_SQL = "('000300.SH', '000905.SH', '000852.SH')"

# Futures filter: only CFFEX (.CFE) contracts and exclude invalid data
FUTURES_WHERE_CLAUSE = """
    (S_INFO_WINDCODE LIKE 'IF%.CFE' OR 
     S_INFO_WINDCODE LIKE 'IC%.CFE' OR 
     S_INFO_WINDCODE LIKE 'IM%.CFE')
"""
# ===========================================

def get_data_from_oracle():
    conn = None
    try:
        # Initialize Oracle client once (ignore error if already initialized)
        try:
            cx_Oracle.init_oracle_client(lib_dir=lib_path)
        except Exception:
            pass

        dsn_tns = cx_Oracle.makedsn(DB_HOST, DB_PORT, service_name=DB_SERVICE)
        conn = cx_Oracle.connect(user=DB_USER, password=DB_PASS, dsn=dsn_tns)
        print("Database connection established. Start downloading 2010–2025 full sample data (V10.0 final version).")

        # -------------------------------------------------------
        # 1. Download spot index daily data (AINDEXEODPRICES)
        # -------------------------------------------------------
        print("[1/6] Downloading spot index data...")
        sql_index = f"""
            SELECT S_INFO_WINDCODE, TRADE_DT, 
                   S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE
            FROM FILESYNC.AINDEXEODPRICES
            WHERE S_INFO_WINDCODE IN {INDEX_CODES_SQL}
              AND TRADE_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
            ORDER BY TRADE_DT
        """
        df_index = pd.read_sql(sql_index, conn)
        df_index.to_csv('all_index_daily.csv', index=False, encoding='utf-8-sig')

        # -------------------------------------------------------
        # 2. Download futures contract static info (CFUTURESCONTPRO)
        # -------------------------------------------------------
        print("[2/6] Downloading futures contract info (static fields)...")
        # Table name and fields are based on the reference schema
        sql_futures_info = f"""
            SELECT S_INFO_WINDCODE, S_INFO_CODE, S_INFO_NAME, 
                   S_INFO_LISTDATE, S_INFO_DELISTDATE, 
                   S_INFO_LTDATED, S_INFO_CEMULTIPLIER, S_INFO_MFPRICE
            FROM FILESYNC.CFUTURESCONTPRO
            WHERE {FUTURES_WHERE_CLAUSE}
        """
        df_info = pd.read_sql(sql_futures_info, conn)
        
        # Rename columns to unified field names for later use
        df_info.rename(columns={
            'S_INFO_LTDATED': 'LAST_TRADE_DATE',
            'S_INFO_CEMULTIPLIER': 'MULTIPLIER',
            'S_INFO_MFPRICE': 'TICK_SIZE',
            'S_INFO_LISTDATE': 'LIST_DATE',
            'S_INFO_DELISTDATE': 'DELIST_DATE'
        }, inplace=True)
        df_info.to_csv('all_futures_info.csv', index=False, encoding='utf-8-sig')

        # -------------------------------------------------------
        # 3. Download futures daily prices (CINDEXFUTURESEODPRICES)
        #    Including settlement price and previous settlement
        # -------------------------------------------------------
        print("[3/6] Downloading futures daily data (including settlement and previous settlement)...")
        # This table contains S_DQ_SETTLE (settlement price) and S_DQ_PRESETTLE (previous settlement)
        sql_futures_daily = f"""
            SELECT S_INFO_WINDCODE, TRADE_DT, 
                   S_DQ_PRESETTLE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE, S_DQ_SETTLE,
                   S_DQ_CHANGE, S_DQ_VOLUME, S_DQ_AMOUNT, S_DQ_OI
            FROM FILESYNC.CINDEXFUTURESEODPRICES
            WHERE {FUTURES_WHERE_CLAUSE}
              AND TRADE_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
        """
        df_daily = pd.read_sql(sql_futures_daily, conn)
        
        df_daily.rename(columns={
            'S_DQ_PRESETTLE': 'PRE_SETTLE',      # Previous settlement (limit calculation base)
            'S_DQ_SETTLE': 'SETTLE_PRICE',       # Current settlement (mark-to-market PnL)
            'S_DQ_AMOUNT': 'TURNOVER_VALUE',     # Turnover value
            'S_DQ_OI': 'OPEN_INTEREST'           # Open interest
        }, inplace=True)

        # Merge static contract info into daily futures data
        # Use inner join to automatically drop contracts not in the static info table
        df_merged = pd.merge(df_daily, df_info, on='S_INFO_WINDCODE', how='inner')
        df_merged.to_csv('all_futures_daily_full.csv', index=False, encoding='utf-8-sig')
        print("    -> Generated all_futures_daily_full.csv (includes settlement price SETTLE_PRICE).")

        # -------------------------------------------------------
        # 4. Official main contract mapping table (CFUTURESCONTRACTMAPPING)
        # -------------------------------------------------------
        print("[4/6] Downloading and generating main contract mapping table...")
        sql_mapping = f"""
            SELECT S_INFO_WINDCODE, FS_MAPPING_WINDCODE, STARTDATE, ENDDATE
            FROM FILESYNC.CFUTURESCONTRACTMAPPING
            WHERE S_INFO_WINDCODE IN ('IF00.CFE', 'IC00.CFE', 'IM00.CFE')
            ORDER BY S_INFO_WINDCODE, STARTDATE
        """
        df_mapping = pd.read_sql(sql_mapping, conn)
        
        all_dates_data = []
        for _, row in df_mapping.iterrows():
            variety = row['S_INFO_WINDCODE'][:2]
            real_code = row['FS_MAPPING_WINDCODE']
            # Expand [STARTDATE, ENDDATE] to daily rows
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
                (df_expanded['TRADE_DT'] >= START_DATE) &
                (df_expanded['TRADE_DT'] <= END_DATE)
            ]
            df_main_contract = df_expanded.pivot_table(
                index='TRADE_DT',
                columns='VARIETY',
                values='MAIN_CODE',
                aggfunc='first'
            )
            df_main_contract.reset_index(inplace=True)
            df_main_contract.to_csv('main_contracts.csv', index=False, encoding='utf-8-sig')

        # -------------------------------------------------------
        # 5. Margin ratios (CFUTURESMARGINRATIO)
        # -------------------------------------------------------
        print("[5/6] Downloading margin ratios...")
        sql_margin = f"""
            SELECT S_INFO_WINDCODE, TRADE_DT, MARGINRATIO, MARGINRATIO_SHORT
            FROM FILESYNC.CFUTURESMARGINRATIO
            WHERE {FUTURES_WHERE_CLAUSE} AND TRADE_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
        """
        df_margin = pd.read_sql(sql_margin, conn)
        df_margin.rename(columns={
            'MARGINRATIO': 'LONG_MARGIN',
            'MARGINRATIO_SHORT': 'SHORT_MARGIN'
        }, inplace=True)
        df_margin.to_csv('all_futures_margin.csv', index=False, encoding='utf-8-sig')

        # -------------------------------------------------------
        # 6. Price limit ratio (CFUTURESPRICECHANGELIMIT)
        # -------------------------------------------------------
        print("[6/6] Downloading price limit data...")
        sql_limits = f"""
            SELECT S_INFO_WINDCODE, PCT_CHG_LIMIT, CHANGE_DT
            FROM FILESYNC.CFUTURESPRICECHANGELIMIT
            WHERE {FUTURES_WHERE_CLAUSE}
        """
        df_limits = pd.read_sql(sql_limits, conn)
        df_limits.rename(columns={
            'PCT_CHG_LIMIT': 'LIMIT_PCT',
            'CHANGE_DT': 'EFFECTIVE_DATE'
        }, inplace=True)
        df_limits.to_csv('all_futures_limits.csv', index=False, encoding='utf-8-sig')

        print("\nAll data files have been successfully downloaded.")

    except Exception as e:
        print(f"\nError occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    get_data_from_oracle()