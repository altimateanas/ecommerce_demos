-- =============================================================================
-- DuckDB Setup Script — CartWave
-- Creates the raw schema and loads CSV seed data for local development.
--
-- Usage: duckdb warehouses/cartwave.duckdb < setup/duckdb/setup.sql
-- =============================================================================

-- Create raw schema for source data
CREATE SCHEMA IF NOT EXISTS raw;

-- =============================================================================
-- Load CSV seed data into raw tables
-- =============================================================================

-- Load customers
CREATE TABLE IF NOT EXISTS raw.customers AS
    SELECT * FROM read_csv_auto('seeds/customers.csv', header=true);

-- Load orders
CREATE TABLE IF NOT EXISTS raw.orders AS
    SELECT * FROM read_csv_auto('seeds/orders.csv', header=true);

-- Load order_items
CREATE TABLE IF NOT EXISTS raw.order_items AS
    SELECT * FROM read_csv_auto('seeds/order_items.csv', header=true);

-- Load products
CREATE TABLE IF NOT EXISTS raw.products AS
    SELECT * FROM read_csv_auto('seeds/products.csv', header=true);

-- Load inventory
CREATE TABLE IF NOT EXISTS raw.inventory AS
    SELECT * FROM read_csv_auto('seeds/inventory.csv', header=true);

-- Load page_views
CREATE TABLE IF NOT EXISTS raw.page_views AS
    SELECT * FROM read_csv_auto('seeds/page_views.csv', header=true);

-- Load returns
CREATE TABLE IF NOT EXISTS raw.returns AS
    SELECT * FROM read_csv_auto('seeds/returns.csv', header=true);

-- Load marketing_spend
CREATE TABLE IF NOT EXISTS raw.marketing_spend AS
    SELECT * FROM read_csv_auto('seeds/marketing_spend.csv', header=true);

-- Load reviews
CREATE TABLE IF NOT EXISTS raw.reviews AS
    SELECT * FROM read_csv_auto('seeds/reviews.csv', header=true);

-- Load warehouses
CREATE TABLE IF NOT EXISTS raw.warehouses AS
    SELECT * FROM read_csv_auto('seeds/warehouses.csv', header=true);

-- =============================================================================
-- Verify row counts
-- =============================================================================
SELECT 'customers' as table_name, count(*) as row_count FROM raw.customers;
SELECT 'orders' as table_name, count(*) as row_count FROM raw.orders;
SELECT 'order_items' as table_name, count(*) as row_count FROM raw.order_items;
SELECT 'products' as table_name, count(*) as row_count FROM raw.products;
SELECT 'inventory' as table_name, count(*) as row_count FROM raw.inventory;
SELECT 'page_views' as table_name, count(*) as row_count FROM raw.page_views;
SELECT 'returns' as table_name, count(*) as row_count FROM raw.returns;
SELECT 'marketing_spend' as table_name, count(*) as row_count FROM raw.marketing_spend;
SELECT 'reviews' as table_name, count(*) as row_count FROM raw.reviews;
SELECT 'warehouses' as table_name, count(*) as row_count FROM raw.warehouses;

-- =============================================================================
-- Create additional schemas for dbt output
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS main;      -- default dbt target schema
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS marts_pii;

-- =============================================================================
-- Summary
-- =============================================================================
-- Tables loaded:
--   raw.customers
--   raw.orders
--   raw.order_items
--   raw.products
--   raw.inventory
--   raw.page_views
--   raw.returns
--   raw.marketing_spend
--   raw.reviews
--   raw.warehouses
-- Run `dbt run --target dev` to build all models against this database.
-- =============================================================================
