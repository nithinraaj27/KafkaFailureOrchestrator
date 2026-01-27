CREATE TABLE failed_events (
    event_id           VARCHAR(100) PRIMARY KEY,
    topic              VARCHAR(100) NOT NULL,
    partition_id       INT NOT NULL,
    offset_id          BIGINT NOT NULL,
    consumer_name      VARCHAR(100) NOT NULL,
    exception_type     VARCHAR(150) NOT NULL,
    error_message      TEXT,
    status             VARCHAR(30) NOT NULL,
    first_failed_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE retry_history (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(100) REFERENCES failed_events(event_id),
    retry_attempt INT NOT NULL,
    retry_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    decision_source VARCHAR(50) NOT NULL
);

CREATE TABLE decision_audit (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(100) REFERENCES failed_events(event_id),
    decision VARCHAR(50) NOT NULL,
    reason TEXT,
    decided_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
