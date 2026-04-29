-- =============================================================================
-- funnel_analysis.sql
-- Funnel conversion analysis with cartesian join between page_views and orders
--
-- Anti-patterns included:
--   - cartesian_join
--   - non_sargable
--   - select_star
--   - union_not_union_all
--   - no_limit
--   - missing_partition_filter
-- =============================================================================

-- ANTI-PATTERN: Cartesian join (comma-separated FROM without proper WHERE join)
-- This produces a cross product of all rows, leading to massive result sets.
SELECT
    *
FROM customers a,
     orders b
WHERE a.created_at >= '2024-01-01'


-- ANTI-PATTERN: Non-sargable predicate — function wrapping indexed column
-- The UPPER() function prevents the database from using an index on the column.
UNION
SELECT
    customer_id,
    customer,
    created_at
FROM customers
WHERE UPPER(customer) = 'ACTIVE'
  AND CAST(created_at AS VARCHAR) LIKE '2024%'
  AND YEAR(updated_at) = 2024
-- ANTI-PATTERN: No LIMIT on an ad-hoc / analysis query

