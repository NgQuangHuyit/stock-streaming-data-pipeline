from cassandra_client import CassandraClient
def create_table():
    with CassandraClient(['localhost'], 'cassandra', 'password123').get_session('stock_market') as session:
        session.execute("""
            CREATE TABLE IF NOT EXISTS stock_market.btc_aggregate (
                symbol text,
                curr_price float,
                utc_timestamp timestamp,
                PRIMARY KEY (symbol, utc_timestamp)
            )
        """)