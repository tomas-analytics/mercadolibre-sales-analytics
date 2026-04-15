# MercadoLibre Sales Analytics

End-to-end analytics project for Mercado Libre sales data using **Python**, **BigQuery**, **dbt** and **Streamlit**.

## Overview

This project aims to build a robust analytics pipeline that processes Excel/CSV sales files exported from Mercado Libre, loads them into a cloud data warehouse, transforms them into an analytical model, and exposes key business metrics through an interactive dashboard.

The main goal is to simulate a real-world analytics workflow with clear separation of layers:

- **Ingestion** with Python
- **Storage** in BigQuery
- **Transformations** with dbt
- **Consumption** through Streamlit

This project is part of a portfolio oriented to Data Analyst / Analytics Engineer roles.

---

## Problem

Mercado Libre sales exports are not stable over time. Across different versions of the files:

- the header row can start in different positions
- the order of columns changes
- some columns appear or disappear
- the financial breakdown becomes more detailed over time

Because of this, direct analysis in spreadsheets or BI tools is fragile and hard to maintain.

---

## Proposed solution

The project implements a robust ingestion and analytics workflow that:

1. detects the header row dynamically
2. normalizes column names
3. maps source columns to an internal standard schema
4. validates required fields
5. performs incremental loading into BigQuery
6. models the data using dbt
7. exposes business metrics through Streamlit dashboards

---

## Project goals

- Build a professional end-to-end analytics project
- Show strong data modeling and ingestion design decisions
- Create a portfolio-ready case that is relevant for interviews
- Design the system so it can scale to other e-commerce sources in the future

---

## Current scope (MVP)

### Included
- Manual upload of Mercado Libre Excel/CSV sales files
- Dynamic header detection
- Column normalization
- YAML-based column mapping
- Required column validation
- Basic type standardization
- Incremental load by `sale_id`
- RAW audit table for load traceability
- dbt layers:
  - staging
  - core
  - marts
- Streamlit app with:
  - upload page
  - dashboard page
  - data quality page

### Not included yet
- Login / multi-user support
- Mercado Libre API integration
- Scheduler / orchestration
- Advanced testing
- Machine learning
- Production deployment

---

## Architecture

```text
Mercado Libre Excel/CSV
        ↓
[ Python Ingestion Layer ]
        ↓
[ BigQuery RAW Layer ]
        ↓
[ dbt Transformations ]
        ↓
[ Streamlit Dashboard ]
Layer responsibilities
1. Ingestion

Python modules are responsible for:

reading files
detecting the header row
normalizing headers
mapping columns to internal names
validating schema
standardizing types
adding technical metadata
loading data incrementally into BigQuery
2. BigQuery

The warehouse stores:

raw_ml_sales: normalized RAW sales data
raw_ml_loads: load audit and traceability data
3. dbt

The transformation layer is divided into:

staging: cleanup, final typing, normalization
core: dimensional model
marts: business-ready analytical tables
4. Streamlit

The app will provide:

file upload
KPI visualization
filters
exploratory analysis
Data grain

1 row = 1 sale

Business key

sale_id

Source column:
# de venta

Internal schema design

Although source files come in Spanish, the internal analytical schema uses English field names.

Examples:

Fecha de venta → sale_date
Ingresos por productos (ARS) → product_revenue_ars
Total (ARS) → total_amount_ars

This decision improves:

consistency
maintainability
scalability
compatibility with standard data tooling
Project structure
mercadolibre-sales-analytics/
├── app/
│   ├── streamlit_app.py
│   ├── pages/
│   └── components/
├── etl/
│   ├── ingest/
│   ├── config/
│   └── utils/
├── transform/
│   └── models/
├── docs/
├── tests/
├── requirements.txt
├── pyproject.toml
├── .gitignore
└── README.md
Documentation

Detailed documentation is available in the /docs folder:

overview.md
arquitectura.md
modelo_datos.md
ingesta.md
roadmap.md
Tech stack
Python
Pandas
PyYAML
BigQuery
dbt
Streamlit
Development status

Phase 1 completed:

project structure
base configuration
documentation
GitHub repository setup

Next step:

Phase 2: ingestion layer implementation
Why this project matters

This project is designed to demonstrate practical skills in:

data ingestion design
schema standardization
analytical modeling
warehouse-first thinking
end-to-end analytics workflows

It is intentionally structured as a real analytics engineering project rather than a simple dashboard over spreadsheets.

Future improvements
automated file ingestion
support for new marketplaces
tests for ingestion modules
better data quality checks
public deployment
richer dashboards and business metrics
Author

Portfolio project developed to strengthen capabilities in analytics engineering, data modeling, and modern data workflows.