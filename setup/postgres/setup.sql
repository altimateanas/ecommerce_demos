-- =============================================================================
-- PostgreSQL Init Script — CartWave
-- This script runs automatically via docker-entrypoint-initdb.d when the
-- postgres-app container starts for the first time.
--
-- Industry: Ecommerce
-- =============================================================================

-- Enable useful extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =============================================================================
-- Create application tables
-- =============================================================================

-- User_accounts table
CREATE TABLE IF NOT EXISTS user_accounts (
    account_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    email VARCHAR(255),
    full_name VARCHAR(255),
    password_hash VARCHAR(255),
    is_verified BOOLEAN,
    last_login_at TIMESTAMP,
    created_at TIMESTAMP
);

-- Wishlists table
CREATE TABLE IF NOT EXISTS wishlists (
    wishlist_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    added_at TIMESTAMP,
    notified_on_sale BOOLEAN
);

-- =============================================================================
-- Load seed data from CSV files (mounted at /seed_data/)
-- =============================================================================

COPY user_accounts
    FROM '/seed_data/user_accounts.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY wishlists
    FROM '/seed_data/wishlists.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- =============================================================================
-- Verify
-- =============================================================================
DO $$
BEGIN
    RAISE NOTICE 'user_accounts: % rows', (SELECT count(*) FROM user_accounts);
END $$;
DO $$
BEGIN
    RAISE NOTICE 'wishlists: % rows', (SELECT count(*) FROM wishlists);
END $$;

-- =============================================================================
-- Grant access to the application user
-- =============================================================================
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cartwave_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO cartwave_user;

-- Done — PostgreSQL app database is ready.
