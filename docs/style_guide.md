# CartWave SQL & dbt Style Guide

> Modern e-commerce platform powering direct-to-consumer brands

This style guide defines conventions for all SQL, dbt models, and analytics code
in the **cartwave-analytics** repository. Consistency reduces cognitive load and makes
code reviews faster.

---

## Table of Contents

1. [Naming Conventions](#naming-conventions)
2. [Column Ordering](#column-ordering)
3. [CTE Pattern](#cte-pattern)
4. [SQL Formatting](#sql-formatting)
5. [Data Type Conventions](#data-type-conventions)
6. [Testing Standards](#testing-standards)
7. [Business Glossary](#business-glossary)

---

## Naming Conventions

### Model Prefixes

| Layer          | Prefix   | Example                           | Materialization |
|----------------|----------|-----------------------------------|-----------------|
| Staging        | `stg_`   | `stg_customers`  | view            |
| Intermediate   | `int_`   | `int_customer_joined`   | table           |
| Mart           | `mart_`  | `mart_customer_daily`  | table           |
| PII-restricted | `mart_`  | `mart_customer_pii`   | table (restricted schema) |

### General Rules

- **snake_case** everywhere: table names, column names, CTEs, macros.
- **No abbreviations** unless universally understood (`id`, `url`, `sku`).
- **Boolean columns**: prefix with `is_`, `has_`, or `was_` (e.g., `is_active`, `has_refund`).
- **Date columns**: suffix with `_at` for timestamps, `_date` for dates (e.g., `created_at`, `order_date`).
- **Amount columns**: suffix with `_amount` or `_cents` to clarify units (e.g., `total_amount`, `price_cents`).
- **Count columns**: suffix with `_count` (e.g., `order_count`, `line_item_count`).

### Source & Seed Names

- Source tables keep their original names in the `raw` schema.
- Seeds use snake_case matching the CSV filename.

---

## Column Ordering

Every SELECT should follow this ordering:

```sql
select
    -- 1. Primary keys
    customer_id,

    -- 2. Foreign keys
    related_entity_id,

    -- 3. Dimensions (categorical/descriptive)
    status,
    category,
    name,

    -- 4. Measures / metrics (numeric)
    total_amount,
    quantity,
    discount_amount,

    -- 5. PII / sensitive fields (email addresses, shipping addresses, payment methods, browsing behavior)
    -- CCPA / GDPR: group these together for easy auditing
    customer_email,
    customer_name,

    -- 6. Timestamps (oldest to newest)
    created_at,
    updated_at

from ...
```

---

## CTE Pattern

All models **must** use the CTE (Common Table Expression) pattern. No nested subqueries.

```sql
with source as (

    select * from {{ source('raw', 'table_name') }}

),

renamed as (

    select
        id as customer_id,
        ...
    from source

),

final as (

    select
        ...
    from renamed
    where is_deleted = false

)

select * from final
```

### CTE Rules

- **First CTE**: always named `source` (for staging) or the primary dependency name.
- **Last CTE**: always named `final`.
- **Final SELECT**: always `select * from final` (no inline transformations).
- **One blank line** between CTE closing paren and the next CTE comma.
- **Commas**: leading commas inside SELECT, trailing commas between CTEs.

---

## SQL Formatting

### Keywords

- **UPPERCASE** SQL keywords: `SELECT`, `FROM`, `WHERE`, `JOIN`, `GROUP BY`, `ORDER BY`.
- **lowercase** column names, table names, aliases.

### Indentation

- **4 spaces** (no tabs).
- Indent column lists inside SELECT.
- Indent JOIN conditions.

### JOINs

```sql
-- Good: explicit join type + ON clause
from orders
left join customers
    on orders.customer_id = customers.customer_id

-- Bad: implicit join (cartesian risk)
from orders, customers
where orders.customer_id = customers.customer_id
```

### WHERE Clauses

```sql
where
    status = 'active'
    and created_at >= '2024-01-01'
    and total_amount > 0
```

---

## Data Type Conventions

| Concept       | Type               | Example                   |
|---------------|--------------------|---------------------------|
| Primary key   | `VARCHAR` / `TEXT`  | `'ORD_00000123'`          |
| Money         | `INTEGER` (cents)  | `4999` = $49.99           |
| Percentage    | `NUMERIC(5,2)`     | `12.50` = 12.5%           |
| Boolean       | `BOOLEAN`          | `true` / `false`          |
| Timestamp     | `TIMESTAMP`        | `2024-01-15 14:30:00`     |
| Date          | `DATE`             | `2024-01-15`              |
| SKU           | `VARCHAR(50)`      | `'SKU-WIDGET-001'`        |

### Money Convention

Store all monetary values as **integers in cents** (or the smallest currency unit).
Use the `cents_to_dollars` macro for display:

```sql
{{ cents_to_dollars('total_amount') }} as total_dollars
```

---

## Testing Standards

### Required Tests

| Column Type    | Required Tests                    |
|----------------|-----------------------------------|
| Primary key    | `unique`, `not_null`              |
| Foreign key    | `not_null`, `relationships`       |
| Status/enum    | `accepted_values`                 |
| Amount         | `not_null`, `dbt_utils.accepted_range` (min: 0) |
| Date           | `not_null`                        |

### Test Coverage Target

- **100%** of primary keys tested for `unique` + `not_null`.
- **100%** of mart models have schema YAML entries.
- All CCPA / GDPR sensitive columns must have masking policy metadata.

---

## Business Glossary

| Term               | Definition                                                |
|--------------------|-----------------------------------------------------------|
| Order              | A customer purchase containing one or more line items     |
| SKU                | Stock Keeping Unit — unique product variant identifier    |
| Cart Abandonment   | Customer adds items but does not complete checkout        |
| GMV                | Gross Merchandise Value — total sales before returns      |
| AOV                | Average Order Value                                       |
| COGS               | Cost of Goods Sold                                        |
| Fulfillment        | Process of picking, packing, and shipping an order        |
| Return Rate        | Percentage of orders returned by customers                |
| LTV                | Lifetime Value — predicted total revenue from a customer  |
| Conversion Rate    | Percentage of visitors who make a purchase                |

---

_This style guide is maintained by the CartWave data team.
Last updated: {{ run_started_at.strftime('%Y-%m-%d') if run_started_at is defined else 'auto-generated' }}_
