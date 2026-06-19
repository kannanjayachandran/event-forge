CREATE TABLE events_raw
(
    timestamp DateTime64(9),

    event_id String,
    session_id String,
    user_id String,

    event_type LowCardinality(String),

    data String
)
ENGINE = MergeTree
ORDER BY (timestamp, event_type);



CREATE TABLE kafka_events
(
    event_id String,
    timestamp String,
    session_id String,
    user_id String,
    event_type String,

    data String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'ecommerce-events',
    kafka_group_name = 'clickhouse-consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;



CREATE MATERIALIZED VIEW mv_events
TO events_raw
AS
SELECT
    parseDateTime64BestEffort(timestamp, 9) AS timestamp,

    event_id,
    session_id,
    user_id,

    event_type,

    data
FROM kafka_events;



CREATE TABLE metrics_per_minute
(
    minute DateTime64(0),

    total_events UInt64,

    landing_events UInt64,
    search_events UInt64,
    product_view_events UInt64,
    add_to_cart_events UInt64,
    checkout_events UInt64,
    purchase_events UInt64
)
ENGINE = SummingMergeTree
ORDER BY minute;



CREATE MATERIALIZED VIEW mv_metrics_per_minute
TO metrics_per_minute
AS
SELECT
    toStartOfMinute(timestamp) AS minute,

    count() AS total_events,

    countIf(event_type = 'landing') AS landing_events,
    countIf(event_type = 'search') AS search_events,
    countIf(event_type = 'product_view') AS product_view_events,
    countIf(event_type = 'add_to_cart') AS add_to_cart_events,
    countIf(event_type = 'checkout') AS checkout_events,
    countIf(event_type = 'purchase') AS purchase_events

FROM events_raw
GROUP BY minute;