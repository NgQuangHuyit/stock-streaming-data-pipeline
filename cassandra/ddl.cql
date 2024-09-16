CREATE KEYSPACE IF NOT EXISTS stock_market WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
CREATE TABLE IF NOT EXISTS stock_market.stock_price (
    symbol text,
    utc_timestamp timestamp,
    curr_price double,
    PRIMARY KEY (symbol, utc_timestamp)
) WITH CLUSTERING ORDER BY (utc_timestamp ASC );

CREATE TABLE IF NOT EXISTS stock_market.stock_cumulative_volume (
    symbol text,
    utc_timestamp timestamp,
    cum_volume double,
    PRIMARY KEY (symbol, utc_timestamp)
) WITH CLUSTERING ORDER BY (utc_timestamp ASC );

CREATE TABLE IF NOT EXISTS stock_market.btc_aggregate (
    symbol text,
    utc_start_ts timestamp,
    utc_end_ts timestamp,
    total_usd_volume double,
    total_btc_volume double,
    high double,
    low double,
    close double,
    num_trades int,
    PRIMARY KEY (symbol, utc_end_ts)
) WITH CLUSTERING ORDER BY (utc_end_ts ASC );

CREATE TABLE IF NOT EXISTS stock_market.btc_predict (
    symbol text,
    utc_timestamp timestamp,
    curr_price double,
    predict_price double,
    PRIMARY KEY (symbol, utc_timestamp)
) WITH CLUSTERING ORDER BY (utc_timestamp ASC );