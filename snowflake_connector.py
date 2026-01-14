import os

from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

USER = os.getenv('SNOWFLAKE_USER')
PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )

def main():
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM WALMART_DB.SILVER.STORE LIMIT 10')
    for row in cursor:
        print(row)
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()