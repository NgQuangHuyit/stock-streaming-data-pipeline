CREATE KEYSPACE IF NOT EXISTS stock_market WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
CREATE TABLE IF NOT EXISTS stock_market.price_tracking (
    symbol text,
    timestamp timestamp,
    cumulative_volume double,
    price double,
    PRIMARY KEY (symbol, timestamp)
) WITH CLUSTERING ORDER BY (timestamp ASC );

CREATE TABLE IF NOT EXISTS stock_market.volume_tracking (
    symbol text,
    timestamp timestamp,
    total_volume double,
    total_usd_volume double,
    PRIMARY KEY (symbol, timestamp)
) WITH CLUSTERING ORDER BY (timestamp ASC );


CREATE TABLE IF NOT EXISTS stock_market.btc_predict (
    symbol text,
    timestamp timestamp,
    curr_price double,
    predict_price double,
    PRIMARY KEY (symbol, timestamp)
) WITH CLUSTERING ORDER BY (timestamp ASC );