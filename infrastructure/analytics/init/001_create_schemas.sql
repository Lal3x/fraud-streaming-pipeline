CREATE SCHEMA IF NOT EXISTS loading;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS loading.transactions_stage (
    load_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    event_id TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    step INTEGER NOT NULL,
    transaction_type TEXT NOT NULL,
    amount NUMERIC(20, 2) NOT NULL,
    origin_account TEXT NOT NULL,
    origin_old_balance NUMERIC(20, 2) NOT NULL,
    origin_new_balance NUMERIC(20, 2) NOT NULL,
    destination_account TEXT NOT NULL,
    destination_old_balance NUMERIC(20, 2) NOT NULL,
    destination_new_balance NUMERIC(20, 2) NOT NULL,
    is_fraud BOOLEAN NOT NULL,
    is_flagged_fraud BOOLEAN NOT NULL,
    event_date DATE NOT NULL,
    event_hour SMALLINT NOT NULL,
    is_merchant_destination BOOLEAN NOT NULL,
    origin_balance_change NUMERIC(20, 2) NOT NULL,
    destination_balance_change NUMERIC(20, 2) NOT NULL,
    expected_origin_balance NUMERIC(20, 2) NOT NULL,
    origin_balance_difference NUMERIC(20, 2) NOT NULL,
    has_origin_balance_anomaly BOOLEAN NOT NULL,
    has_destination_balance_anomaly BOOLEAN NOT NULL,
    is_origin_account_drained BOOLEAN NOT NULL,
    processing_latency_seconds BIGINT NOT NULL,
    amount_range TEXT NOT NULL,
    rule_high_amount BOOLEAN NOT NULL,
    rule_risky_transaction_type BOOLEAN NOT NULL,
    rule_origin_account_drained BOOLEAN NOT NULL,
    rule_origin_balance_anomaly BOOLEAN NOT NULL,
    triggered_rules_json TEXT NOT NULL,
    risk_score SMALLINT NOT NULL,
    risk_level TEXT NOT NULL,
    predicted_fraud BOOLEAN NOT NULL,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_timestamp TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL,
    ingestion_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.transactions (
    event_id UUID PRIMARY KEY,
    source_record_id TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    step INTEGER NOT NULL CHECK (step >= 1),
    transaction_type TEXT NOT NULL CHECK (
        transaction_type IN ('CASH_IN', 'CASH_OUT', 'DEBIT', 'PAYMENT', 'TRANSFER')
    ),
    amount NUMERIC(20, 2) NOT NULL CHECK (amount >= 0),
    origin_account TEXT NOT NULL,
    origin_old_balance NUMERIC(20, 2) NOT NULL CHECK (origin_old_balance >= 0),
    origin_new_balance NUMERIC(20, 2) NOT NULL CHECK (origin_new_balance >= 0),
    destination_account TEXT NOT NULL,
    destination_old_balance NUMERIC(20, 2) NOT NULL CHECK (destination_old_balance >= 0),
    destination_new_balance NUMERIC(20, 2) NOT NULL CHECK (destination_new_balance >= 0),
    is_fraud BOOLEAN NOT NULL,
    is_flagged_fraud BOOLEAN NOT NULL,
    event_date DATE NOT NULL,
    event_hour SMALLINT NOT NULL CHECK (event_hour BETWEEN 0 AND 23),
    is_merchant_destination BOOLEAN NOT NULL,
    origin_balance_change NUMERIC(20, 2) NOT NULL,
    destination_balance_change NUMERIC(20, 2) NOT NULL,
    expected_origin_balance NUMERIC(20, 2) NOT NULL,
    origin_balance_difference NUMERIC(20, 2) NOT NULL,
    has_origin_balance_anomaly BOOLEAN NOT NULL,
    has_destination_balance_anomaly BOOLEAN NOT NULL,
    is_origin_account_drained BOOLEAN NOT NULL,
    processing_latency_seconds BIGINT NOT NULL CHECK (processing_latency_seconds >= 0),
    amount_range TEXT NOT NULL CHECK (amount_range IN ('SMALL', 'MEDIUM', 'LARGE', 'VERY_LARGE')),
    rule_high_amount BOOLEAN NOT NULL,
    rule_risky_transaction_type BOOLEAN NOT NULL,
    rule_origin_account_drained BOOLEAN NOT NULL,
    rule_origin_balance_anomaly BOOLEAN NOT NULL,
    triggered_rules TEXT[] NOT NULL DEFAULT '{}',
    risk_score SMALLINT NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
    risk_level TEXT NOT NULL CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH')),
    predicted_fraud BOOLEAN NOT NULL,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_timestamp TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL,
    ingestion_date DATE NOT NULL,
    source_file TEXT NOT NULL,
    load_id UUID NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS loading.fraud_alerts_stage (
    load_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    transaction_type TEXT NOT NULL,
    amount NUMERIC(20, 2) NOT NULL,
    origin_account TEXT NOT NULL,
    destination_account TEXT NOT NULL,
    triggered_rules_json TEXT NOT NULL,
    risk_score SMALLINT NOT NULL,
    risk_level TEXT NOT NULL,
    predicted_fraud BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.fraud_alerts (
    event_id UUID PRIMARY KEY REFERENCES silver.transactions(event_id),
    event_time TIMESTAMPTZ NOT NULL,
    transaction_type TEXT NOT NULL,
    amount NUMERIC(20, 2) NOT NULL CHECK (amount >= 0),
    origin_account TEXT NOT NULL,
    destination_account TEXT NOT NULL,
    triggered_rules TEXT[] NOT NULL DEFAULT '{}',
    risk_score SMALLINT NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
    risk_level TEXT NOT NULL CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH')),
    predicted_fraud BOOLEAN NOT NULL CHECK (predicted_fraud),
    source_file TEXT NOT NULL,
    load_id UUID NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS monitoring.loaded_files (
    dataset_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    load_id UUID NOT NULL,
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_name, source_file)
);

CREATE TABLE IF NOT EXISTS monitoring.pipeline_metrics (
    load_id UUID PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    files_discovered INTEGER NOT NULL DEFAULT 0,
    files_processed INTEGER NOT NULL DEFAULT 0,
    records_read BIGINT NOT NULL DEFAULT 0,
    records_valid BIGINT NOT NULL DEFAULT 0,
    records_rejected BIGINT NOT NULL DEFAULT 0,
    records_inserted BIGINT NOT NULL DEFAULT 0,
    records_duplicated BIGINT NOT NULL DEFAULT 0,
    alerts_read BIGINT NOT NULL DEFAULT 0,
    alerts_inserted BIGINT NOT NULL DEFAULT 0,
    error_message TEXT,
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE TABLE IF NOT EXISTS monitoring.ml_model_runs (
    run_id UUID PRIMARY KEY,
    model_version TEXT NOT NULL UNIQUE,
    model_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    training_cutoff TIMESTAMPTZ,
    train_row_count BIGINT NOT NULL DEFAULT 0,
    evaluation_row_count BIGINT NOT NULL DEFAULT 0,
    scored_row_count BIGINT NOT NULL DEFAULT 0,
    anomaly_row_count BIGINT NOT NULL DEFAULT 0,
    contamination DOUBLE PRECISION NOT NULL CHECK (contamination > 0 AND contamination <= 0.5),
    train_fraction DOUBLE PRECISION NOT NULL CHECK (train_fraction > 0 AND train_fraction < 1),
    random_state INTEGER NOT NULL,
    feature_names JSONB NOT NULL,
    error_message TEXT,
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE TABLE IF NOT EXISTS analytics.transaction_anomaly_scores (
    event_id UUID NOT NULL REFERENCES silver.transactions(event_id),
    model_version TEXT NOT NULL REFERENCES monitoring.ml_model_runs(model_version),
    dataset_split TEXT NOT NULL CHECK (dataset_split IN ('TRAIN', 'EVALUATION')),
    anomaly_score DOUBLE PRECISION NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    scored_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_id, model_version)
);

CREATE INDEX IF NOT EXISTS idx_transactions_event_time ON silver.transactions (event_time);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON silver.transactions (transaction_type);
CREATE INDEX IF NOT EXISTS idx_transactions_origin ON silver.transactions (origin_account);
CREATE INDEX IF NOT EXISTS idx_transactions_destination ON silver.transactions (destination_account);
CREATE INDEX IF NOT EXISTS idx_transactions_risk_level ON silver.transactions (risk_level);
CREATE INDEX IF NOT EXISTS idx_transactions_predicted_fraud ON silver.transactions (predicted_fraud);
CREATE INDEX IF NOT EXISTS idx_transactions_is_fraud ON silver.transactions (is_fraud);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_event_time ON silver.fraud_alerts (event_time);
CREATE INDEX IF NOT EXISTS idx_anomaly_scores_event ON analytics.transaction_anomaly_scores (event_id);
CREATE INDEX IF NOT EXISTS idx_anomaly_scores_anomaly ON analytics.transaction_anomaly_scores (is_anomaly);
