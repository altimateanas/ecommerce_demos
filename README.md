# CartWave

> Modern e-commerce platform powering direct-to-consumer brands

A realistic **Ecommerce** analytics demo repository built with dbt, Snowflake, DuckDB, PostgreSQL, and Airflow. This project demonstrates modern data stack patterns including:

- **dbt** models (staging, intermediate, marts) with full test coverage
- **Snowflake** warehouse setup with RBAC and cost governance
- **DuckDB** for zero-setup local development
- **PostgreSQL** for application data (via Docker)
- **Airflow** for orchestration and scheduling
- **CI/CD** with GitHub Actions SQL quality gates

---

## Quick Start

### Prerequisites

- Python 3.10+
- [dbt-core](https://docs.getdbt.com/docs/introduction) 1.7+
- Docker & Docker Compose (for PostgreSQL + Airflow)

### 1. Clone & Install

```bash
git clone https://github.com/your-org/cartwave-analytics.git
cd cartwave-analytics
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Local Development with DuckDB (Fastest)

No database setup needed. DuckDB runs in-process.

```bash
# Load seed data into DuckDB
dbt seed --target dev

# Run all models
dbt run --target dev

# Run tests
dbt test --target dev

# Generate docs
dbt docs generate && dbt docs serve
```

The DuckDB file is created at `warehouses/cartwave.duckdb`.

### 3. Snowflake Setup

```bash
# Set environment variables
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"

# Run the setup script (as ACCOUNTADMIN)
# This creates the database, schemas, warehouses, roles, and grants
snowsql -f setup/snowflake/setup.sql

# Seed + run
dbt seed --target snowflake
dbt run --target snowflake
dbt test --target snowflake
```

### 4. PostgreSQL + Airflow (Docker)

```bash
# Start all services (PostgreSQL, Airflow)
docker-compose up -d

# Wait for Airflow to initialize, then visit:
# http://localhost:8080 (admin / admin)

# Run dbt against PostgreSQL
dbt seed --target postgres
dbt run --target postgres
```

---

## Project Structure

```
cartwave-analytics/
├── models/
│   ├── staging/          # stg_* views — clean & rename raw data
│   ├── intermediate/     # int_* tables — joins & business logic
│   └── marts/            # mart_* tables — final analytical models
├── seeds/                # CSV files loaded as raw data
├── tests/                # Custom dbt tests
├── macros/               # Reusable SQL macros
├── dags/                 # Airflow DAG definitions
├── setup/
│   ├── snowflake/        # Snowflake DDL & RBAC setup
│   ├── duckdb/           # DuckDB initialization
│   └── postgres/         # PostgreSQL init scripts
├── analyses/             # Ad-hoc SQL queries
├── legacy/               # Legacy PySpark ETL (migration reference)
├── .github/workflows/    # CI/CD: SQL quality gate, cost reports
├── dbt_project.yml       # dbt project configuration
├── profiles.yml          # dbt connection profiles
├── docker-compose.yml    # Local dev stack
└── style_guide.md        # SQL & dbt conventions```

---

## Data Model

### Staging Layer (`stg_*`)

Staging models are 1:1 with source tables. They clean, rename, and type-cast columns.

| Model | Source Table | Description |
|-------|-------------|-------------|
| `stg_customers` | `customers` | Staged customers data |
| `stg_orders` | `orders` | Staged orders data |
| `stg_order_items` | `order_items` | Staged order_items data |
| `stg_products` | `products` | Staged products data |
| `stg_inventory` | `inventory` | Staged inventory data |
| `stg_page_views` | `page_views` | Staged page_views data |
| `stg_returns` | `returns` | Staged returns data |
| `stg_marketing_spend` | `marketing_spend` | Staged marketing_spend data |
| `stg_reviews` | `reviews` | Staged reviews data |
| `stg_warehouses` | `warehouses` | Staged warehouses data |

### Intermediate Layer (`int_*`)

Intermediate models join staging tables and apply business logic.

| Model | Description |
|-------|-------------|
| `int_order_enriched` | Orders with customer demographics and product details |
| `int_customer_lifetime` | Customer lifetime value, order frequency, return rate |
| `int_product_performance` | Product-level metrics: revenue, units, avg rating, return rate |
| `int_session_funnel` | Sessionized funnel: home → category → product → cart → checkout |
| `int_marketing_attribution` | Multi-touch attribution: first-click, last-click, linear |
| `int_inventory_health` | Stock levels, days of supply, reorder alerts |

### Marts Layer (`mart_*`)

Marts are the final analytical tables consumed by BI tools and stakeholders.

| Model | Description |
|-------|-------------|
| `mart_customer_cohorts` | Cohort retention analysis by acquisition month and channel |
| `mart_product_catalog` | Product performance with inventory status |
| `mart_marketing_roi` | ROAS and CAC by channel and campaign |
| `mart_return_analysis` | Return rates by product, reason, and customer segment |
| `{'name': 'mart_daily_sales', 'deps': ['stg_orders', 'stg_order_items', 'stg_products', 'stg_customers'], 'description': 'Daily sales summary with product and channel breakdowns', 'metrics': ['gross_revenue', 'net_revenue', 'order_count', 'units_sold', 'avg_order_value', 'unique_customers'], 'dimensions': ['date', 'product_category', 'customer_segment', 'acquisition_channel', 'shipping_method']}` | _(exists but missing schema/tests)_ |

**PII Models** (restricted access, CCPA / GDPR):

| Model | Contains |
|-------|----------|
| `{'name': 'mart_customer_360', 'deps': ['stg_customers', 'stg_orders', 'stg_page_views', 'stg_returns', 'stg_reviews'], 'description': 'Full customer view with purchase history, browsing behavior, and preferences', 'pii_columns': ['email', 'full_name', 'phone', 'shipping_address', 'billing_address'], 'dependencies': ['stg_customers', 'stg_orders', 'stg_page_views', 'stg_returns', 'stg_reviews']}` | email addresses, shipping addresses, payment methods, browsing behavior |

---

## Compliance & Governance

This project is designed with **CCPA / GDPR** compliance in mind:

- **SOC 2** controls for customer data protection
- PII fields (email, name, address) in restricted schema
- Data retention policies enforced via dbt snapshots

---

## Airflow DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `daily_ecommerce_etl` | `0 6 * * *` | Daily CartWave data pipeline |
| `nightly_inventory_refresh` | `0 2 * * *` | Nightly inventory sync and reorder alert generation |
| `weekly_marketing_attribution` | `0 8 * * 1` | Weekly marketing attribution and ROI analysis |
| `hourly_cart_abandonment` | `0 * * * *` | Hourly cart abandonment detection and recovery triggers |

---

## Known Issues (Intentional)

This demo repo contains **intentional issues** designed to be discovered and fixed
using data tooling:

1. **Missing mart schema** - `{'name': 'mart_daily_sales', 'deps': ['stg_orders', 'stg_order_items', 'stg_products', 'stg_customers'], 'description': 'Daily sales summary with product and channel breakdowns', 'metrics': ['gross_revenue', 'net_revenue', 'order_count', 'units_sold', 'avg_order_value', 'unique_customers'], 'dimensions': ['date', 'product_category', 'customer_segment', 'acquisition_channel', 'shipping_method']}` has no YAML schema entry or tests
2. **Snowflake dialect models** - Some staging models use Snowflake-specific functions that fail on DuckDB
3. **RBAC flaw** - The `PII_VIEWER` role is incorrectly granted to all analysts
4. **Cost waste** - `TEMP_WH` warehouse has `AUTO_SUSPEND = 0`
5. **Bad queries** - Files in `analyses/` contain SQL anti-patterns
6. **Legacy ETL** - PySpark scripts in `legacy/` need migration to dbt

---

## Contributing

See [style_guide.md](style_guide.md) for SQL and dbt conventions.

---

_Generated for the CartWave demo. Industry: Ecommerce._
