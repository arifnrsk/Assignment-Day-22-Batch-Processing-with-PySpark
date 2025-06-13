-- Create business database if not exists
-- This script runs in the business PostgreSQL container

-- Create tables for batch processing results
CREATE TABLE IF NOT EXISTS customer_summary (
    loyalty_number BIGINT PRIMARY KEY,
    loyalty_card VARCHAR(50),
    education VARCHAR(100),
    gender VARCHAR(20),
    total_flights_lifetime BIGINT,
    total_distance_km DOUBLE PRECISION,
    total_points_earned DOUBLE PRECISION,
    total_points_used DOUBLE PRECISION,
    avg_salary DOUBLE PRECISION,
    total_activity_periods BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS monthly_trends (
    id SERIAL PRIMARY KEY,
    year INTEGER,
    month INTEGER,
    monthly_total_flights BIGINT,
    monthly_avg_flights_per_customer DOUBLE PRECISION,
    unique_customers BIGINT,
    monthly_total_distance DOUBLE PRECISION,
    monthly_points_earned DOUBLE PRECISION,
    monthly_points_redeemed DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, month)
);

CREATE TABLE IF NOT EXISTS loyalty_card_performance (
    id SERIAL PRIMARY KEY,
    loyalty_card VARCHAR(50) UNIQUE,
    unique_customers BIGINT,
    avg_flights_per_period DOUBLE PRECISION,
    avg_points_per_period DOUBLE PRECISION,
    avg_distance_per_period DOUBLE PRECISION,
    total_flights BIGINT,
    total_points_earned DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS churn_analysis (
    id SERIAL PRIMARY KEY,
    loyalty_number BIGINT,
    loyalty_card VARCHAR(50),
    education VARCHAR(100),
    gender VARCHAR(20),
    last_flight_year INTEGER,
    last_flight_month INTEGER,
    months_since_last_flight INTEGER,
    total_lifetime_flights BIGINT,
    total_lifetime_points DOUBLE PRECISION,
    avg_flights_per_month DOUBLE PRECISION,
    churn_risk_score DOUBLE PRECISION,
    churn_category VARCHAR(50),
    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS retention_analysis (
    id SERIAL PRIMARY KEY,
    cohort_month VARCHAR(20),
    period_number INTEGER,
    customers_in_cohort BIGINT,
    customers_active BIGINT,
    retention_rate DOUBLE PRECISION,
    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(cohort_month, period_number)
);

CREATE TABLE IF NOT EXISTS batch_processing_log (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    execution_date TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50),
    records_processed BIGINT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_customer_summary_loyalty_card ON customer_summary(loyalty_card);
CREATE INDEX IF NOT EXISTS idx_customer_summary_education ON customer_summary(education);
CREATE INDEX IF NOT EXISTS idx_monthly_trends_year_month ON monthly_trends(year, month);
CREATE INDEX IF NOT EXISTS idx_churn_analysis_risk_score ON churn_analysis(churn_risk_score);
CREATE INDEX IF NOT EXISTS idx_churn_analysis_category ON churn_analysis(churn_category);
CREATE INDEX IF NOT EXISTS idx_retention_analysis_cohort ON retention_analysis(cohort_month);

-- Insert sample data or initial configurations if needed
INSERT INTO batch_processing_log (dag_id, task_id, execution_date, start_time, end_time, status, records_processed, error_message)
VALUES ('initialization', 'database_setup', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'SUCCESS', 0, 'Database initialized successfully')
ON CONFLICT DO NOTHING; 