-- =============================================================================
-- inventory_report.sql
-- Inventory report using correlated subqueries instead of window functions
--
-- Anti-patterns included:
--   - select_star
--   - unnecessary_subqueries
--   - correlated_subquery
-- =============================================================================

-- ANTI-PATTERN: SELECT * — retrieves all columns including unnecessary data
-- This prevents pushdown optimization and makes queries fragile to schema changes.
SELECT *
FROM customers
LIMIT 1000

