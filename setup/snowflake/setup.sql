-- =============================================================================
-- Snowflake Setup Script — CartWave
-- Industry: Ecommerce
-- Compliance: CCPA / GDPR
--
-- Run this script as ACCOUNTADMIN or SYSADMIN to set up the demo environment.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. DATABASE & SCHEMAS
-- =============================================================================

CREATE DATABASE IF NOT EXISTS CARTWAVE;
USE DATABASE CARTWAVE;

CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw ingested data from source systems';
CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Cleaned and standardized staging models (views)';
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE
    COMMENT = 'Business logic transformations (tables)';
CREATE SCHEMA IF NOT EXISTS MARTS
    COMMENT = 'Final analytical models consumed by BI tools';
CREATE SCHEMA IF NOT EXISTS MARTS_PII
    COMMENT = 'CCPA / GDPR restricted — contains email addresses, shipping addresses, payment methods, browsing behavior data';

-- =============================================================================
-- 2. WAREHOUSES
-- =============================================================================

-- Analytics warehouse for BI queries and dbt runs
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
    WITH WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Primary analytics warehouse for dbt and BI tools';

-- Ingestion warehouse for data loading
CREATE WAREHOUSE IF NOT EXISTS INGESTION_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Data ingestion and loading warehouse';

-- Reporting warehouse for dashboards
CREATE WAREHOUSE IF NOT EXISTS REPORTING_WH
    WITH WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 600
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Reporting warehouse for dashboards and scheduled queries';

-- !! INTENTIONAL FLAW: This warehouse has no AUTO_SUSPEND !!
-- This is a cost governance issue — the warehouse will run indefinitely
-- until manually suspended, accumulating unnecessary Snowflake credits.
-- Altimate AI / SDF should flag this in the cost optimization workflow.
CREATE WAREHOUSE IF NOT EXISTS TEMP_WH
    WITH WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 0
    AUTO_RESUME = TRUE
    COMMENT = 'Temporary warehouse — WARNING: no auto-suspend configured';

-- =============================================================================
-- 3. ROLES (RBAC Hierarchy)
-- =============================================================================

CREATE ROLE IF NOT EXISTS DATA_ADMIN
    COMMENT = 'Full admin access to CARTWAVE';
CREATE ROLE IF NOT EXISTS DATA_ENGINEER
    COMMENT = 'Data engineering — can create/modify models';
CREATE ROLE IF NOT EXISTS ANALYST
    COMMENT = 'Analytics — read access to staging, intermediate, marts';
CREATE ROLE IF NOT EXISTS VIEWER
    COMMENT = 'Read-only access to marts only';
CREATE ROLE IF NOT EXISTS PII_VIEWER
    COMMENT = 'CCPA / GDPR — access to email addresses, shipping addresses, payment methods, browsing behavior data in MARTS_PII';

-- Role hierarchy
GRANT ROLE VIEWER TO ROLE ANALYST;
GRANT ROLE ANALYST TO ROLE DATA_ENGINEER;
GRANT ROLE DATA_ENGINEER TO ROLE DATA_ADMIN;
GRANT ROLE DATA_ADMIN TO ROLE SYSADMIN;

-- !! INTENTIONAL RBAC FLAW !!
-- The PII_VIEWER role is granted directly to ANALYST instead of requiring
-- explicit PII access approval. This means ANY analyst can see email addresses, shipping addresses, payment methods, browsing behavior
-- data without going through the CCPA / GDPR access request process.
-- This is a compliance violation that should be caught during governance review.
GRANT ROLE PII_VIEWER TO ROLE ANALYST;
-- ^ FIX: This should NOT be here. PII_VIEWER should be granted individually
--   after CCPA / GDPR compliance training + manager approval.
--   Correct: GRANT ROLE PII_VIEWER TO USER <specific_user>;

-- =============================================================================
-- 4. USERS
-- =============================================================================

CREATE USER IF NOT EXISTS eng_user
    PASSWORD = 'ChangeMe123!'
    DEFAULT_ROLE = DATA_ENGINEER
    DEFAULT_WAREHOUSE = ANALYTICS_WH
    COMMENT = 'Demo user';
GRANT ROLE DATA_ENGINEER TO USER eng_user;

CREATE USER IF NOT EXISTS analyst_user
    PASSWORD = 'ChangeMe123!'
    DEFAULT_ROLE = DATA_ANALYST
    DEFAULT_WAREHOUSE = ANALYTICS_WH
    COMMENT = 'Demo user';
GRANT ROLE DATA_ANALYST TO USER analyst_user;

CREATE USER IF NOT EXISTS marketing_user
    PASSWORD = 'ChangeMe123!'
    DEFAULT_ROLE = MARKETING_ANALYST
    DEFAULT_WAREHOUSE = ANALYTICS_WH
    COMMENT = 'Demo user';
GRANT ROLE MARKETING_ANALYST TO USER marketing_user;

CREATE USER IF NOT EXISTS support_user
    PASSWORD = 'ChangeMe123!'
    DEFAULT_ROLE = SUPPORT_ROLE
    DEFAULT_WAREHOUSE = ANALYTICS_WH
    COMMENT = 'Demo user';
GRANT ROLE SUPPORT_ROLE TO USER support_user;

CREATE USER IF NOT EXISTS intern_user
    PASSWORD = 'ChangeMe123!'
    DEFAULT_ROLE = INTERN_ROLE
    DEFAULT_WAREHOUSE = ANALYTICS_WH
    COMMENT = 'Demo user';
GRANT ROLE INTERN_ROLE TO USER intern_user;

-- =============================================================================
-- 5. GRANTS
-- =============================================================================

-- Admin: full access
GRANT ALL ON DATABASE CARTWAVE TO ROLE DATA_ADMIN;
GRANT ALL ON ALL SCHEMAS IN DATABASE CARTWAVE TO ROLE DATA_ADMIN;
GRANT ALL ON WAREHOUSE ANALYTICS_WH TO ROLE DATA_ADMIN;
GRANT ALL ON WAREHOUSE INGESTION_WH TO ROLE DATA_ADMIN;
GRANT ALL ON WAREHOUSE REPORTING_WH TO ROLE DATA_ADMIN;
GRANT ALL ON WAREHOUSE TEMP_WH TO ROLE DATA_ADMIN;

-- Engineer: create + modify in all schemas
GRANT USAGE ON DATABASE CARTWAVE TO ROLE DATA_ENGINEER;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON ALL SCHEMAS IN DATABASE CARTWAVE TO ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE INGESTION_WH TO ROLE DATA_ENGINEER;

-- Analyst: read staging + intermediate + marts
GRANT USAGE ON DATABASE CARTWAVE TO ROLE ANALYST;
GRANT USAGE ON SCHEMA CARTWAVE.STAGING TO ROLE ANALYST;
GRANT USAGE ON SCHEMA CARTWAVE.INTERMEDIATE TO ROLE ANALYST;
GRANT USAGE ON SCHEMA CARTWAVE.MARTS TO ROLE ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA CARTWAVE.STAGING TO ROLE ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA CARTWAVE.STAGING TO ROLE ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA CARTWAVE.INTERMEDIATE TO ROLE ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA CARTWAVE.MARTS TO ROLE ANALYST;
GRANT USAGE ON WAREHOUSE REPORTING_WH TO ROLE ANALYST;

-- Viewer: read marts only
GRANT USAGE ON DATABASE CARTWAVE TO ROLE VIEWER;
GRANT USAGE ON SCHEMA CARTWAVE.MARTS TO ROLE VIEWER;
GRANT SELECT ON ALL TABLES IN SCHEMA CARTWAVE.MARTS TO ROLE VIEWER;
GRANT USAGE ON WAREHOUSE REPORTING_WH TO ROLE VIEWER;

-- PII Viewer: access to MARTS_PII schema
GRANT USAGE ON DATABASE CARTWAVE TO ROLE PII_VIEWER;
GRANT USAGE ON SCHEMA CARTWAVE.MARTS_PII TO ROLE PII_VIEWER;
GRANT SELECT ON ALL TABLES IN SCHEMA CARTWAVE.MARTS_PII TO ROLE PII_VIEWER;
GRANT USAGE ON WAREHOUSE REPORTING_WH TO ROLE PII_VIEWER;

-- Future grants (auto-apply to new objects)
GRANT SELECT ON FUTURE TABLES IN SCHEMA CARTWAVE.MARTS TO ROLE ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CARTWAVE.MARTS TO ROLE VIEWER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA CARTWAVE.STAGING TO ROLE ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CARTWAVE.MARTS_PII TO ROLE PII_VIEWER;

-- =============================================================================
-- DONE
-- =============================================================================
-- Summary:
--   Database: CARTWAVE
--   Schemas:  RAW, STAGING, INTERMEDIATE, MARTS, MARTS_PII
--   Warehouses: ANALYTICS_WH, INGESTION_WH, REPORTING_WH, TEMP_WH (flawed)
--   Roles: DATA_ADMIN > DATA_ENGINEER > ANALYST > VIEWER, PII_VIEWER (flawed grant)
--
-- Known issues to discover:
--   1. TEMP_WH has AUTO_SUSPEND = 0 (cost waste)
--   2. PII_VIEWER granted to ANALYST (CCPA / GDPR violation)
-- =============================================================================
