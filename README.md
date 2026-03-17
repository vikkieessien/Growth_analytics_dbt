# Growth Analytics dbt Project

A marketing analytics pipeline that transforms raw app data into
business-ready insights on user acquisition, retention and revenue.

Built with **dbt** and **BigQuery**, exposed to **Tableau** for reporting.

---

## What This Project Does

A free-to-play music education app acquires users across multiple marketing
channels. This pipeline answers three core business questions:

- **Which channels deliver the best return?** - CAC and LTV by acquisition channel
- **Are users sticking around?** - Weekly retention curves by platform and cohort
- **How valuable are retained users?** - Revenue per retained user over time

---

## Pipeline Architecture
```
Raw Data (BigQuery)
      ↓
Staging  →  stg_profiles / stg_payments / stg_activity
      ↓
Intermediate  →  int_payment_enriched / int_channel_metrics
      ↓
Marts  →  mart_user_marketing / mart_cohort_retention
      ↓
Tableau Dashboard
```

---

## Key Metrics

| Metric | Definition |
|---|---|
| 90-day LTV | Revenue per user in first 90 days |
| CAC Proxy | Revenue per paying user per channel |
| Predicted CLV | Total revenue per install per channel |
| Retention Rate | Active users/cohort size per week |
| Revenue per Retained User | Weekly revenue / retained users |

---

## Tech Stack

- **dbt** — transformation and testing
- **BigQuery** — cloud data warehouse
- **Tableau** — dashboard and visualisation

---

## Setup
```bash
git clone https://github.com/vikkieessien/Growth_analytics_dbt.git
cd Growth_analytics_dbt
pip install dbt-bigquery
dbt seed    # load raw data
dbt run     # build all models
dbt test    # run quality checks
```

---

## Dashboard

*Coming soon — Tableau dashboard connecting directly to BigQuery mart tables.*

---

## Author

Victoria Essien — Data Analyst
[GitHub](https://github.com/vikkieessien)
