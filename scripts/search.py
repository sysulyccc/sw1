import cx_Oracle
import pandas as pd
import os

# Configuration
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
lib_path = "Your oracle path"
DB_HOST = '219.223.208.52'
DB_PORT = '1521'
DB_SERVICE = 'orcl'
DB_USER = 'Your username'
DB_PASS = 'Your password'

def find_the_table():
    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_path)
    except:
        pass
    
    dsn = cx_Oracle.makedsn(DB_HOST, DB_PORT, service_name=DB_SERVICE)
    conn = cx_Oracle.connect(user=DB_USER, password=DB_PASS, dsn=dsn)
    
    print("Searching for candidate futures price/EOD tables in FILESYNC schema...")

    # Search logic: table name contains 'FUTURE' and 'EOD' (end of day) or 'PRICE'
    sql_search = """
        SELECT OWNER, TABLE_NAME 
        FROM ALL_TABLES 
        WHERE (TABLE_NAME LIKE '%FUTURE%PRICE%' 
           OR TABLE_NAME LIKE '%FUTURE%EOD%')
          AND OWNER = 'FILESYNC'
        ORDER BY TABLE_NAME
    """
    
    df = pd.read_sql(sql_search, conn)
    print("\nFound the following tables:")
    print(df)
    
    conn.close()

if __name__ == '__main__':
    find_the_table()