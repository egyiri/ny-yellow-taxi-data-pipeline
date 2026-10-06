# NYC Yellow Taxi Data Pipeline

An end-to-end batch data pipeline that extracts NYC Yellow Taxi trip data from the NYC Open Data API, cleans and enriches it through a medallion architecture (bronze → silver → gold), enforces data quality rules, and exposes the results for SQL analytics via Amazon Redshift Spectrum — all orchestrated by Apache Airflow and provisioned with Terraform.

## Architecture

![Pipeline architecture](./architecture.png)

The pipeline runs monthly. Each run pulls a sample of that month's trips from the Socrata API, lands them as raw JSON, transforms them into a clean/quarantine silver layer plus three gold aggregate tables, registers everything in the AWS Glue Data Catalog, and runs an automated data quality check before the run is considered complete.

## Tech stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow (`GlueJobOperator`, `GlueDataQualityRuleSetEvaluationRunOperator`) |
| Compute | AWS Glue (PySpark, Glue 3.0) |
| Storage | Amazon S3 (Parquet, Hive-style partitioning) |
| Cataloging | AWS Glue Data Catalog |
| Data quality | AWS Glue Data Quality (DQDL rulesets) |
| Analytics | Amazon Redshift Serverless + Redshift Spectrum |
| Infrastructure as code | Terraform |

## Data source

[NYC Open Data — Yellow Taxi Trip Data](https://data.cityofnewyork.us/resource/4b4i-vvec.json), accessed via the Socrata Open Data API (SODA). Each run filters trips by pickup month and paginates through the API up to a configurable row cap, to keep each run's size and runtime predictable.

## Pipeline stages

### 1. Extract (`ny_yellow_taxi_data_extraction`)
A Glue job calls the Socrata API for the DAG run's month, paginating via `$offset` until it hits the configured row cap or runs out of data. Writes raw JSON to:
```
s3://<bucket>/landing_zone/city_taxi_data/api_data_<timestamp>/
```

### 2. Transform (`ny_yellow_taxi_data_transformation`)
A second Glue job reads every landed extract folder plus a TLC taxi zone lookup table, then:
- Casts raw string fields to proper types and deduplicates records
- Derives trip duration, average speed, pickup hour/day, distance band, tip percentage, and an airport-trip flag
- Runs each row through a set of data quality rules (null timestamps, negative fares, impossible speeds, invalid passenger counts, etc.), splitting output into:
  - **`silver/clean_trips`** — passing rows, partitioned by `pickup_date`, enriched with pickup/dropoff borough and zone names via a broadcast join
  - **`silver/quarantine_trips`** — failing rows, tagged with a `reject_reason`
- Aggregates clean trips into three gold tables:
  - **`gold/zone_hour_earnings`** — revenue per minute by zone, day of week, and hour, ranked within each time slot
  - **`gold/tipping_behavior`** — tip percentage by borough, hour, distance band, and airport trips (credit card payments only)
  - **`gold/dq_scorecard`** — rule-level rejection counts from the quarantine step

All outputs are written as Parquet.

### 3. Catalog
`clean_trips`, `quarantine_trips`, and the three gold tables are registered as external tables in a Glue Data Catalog database (`ny_taxi_db`), provisioned via Terraform. New `pickup_date` partitions are registered with an Athena `MSCK REPAIR TABLE` statement after each transform run.

### 4. Data quality gate
An AWS Glue Data Quality ruleset (`ny_yellow_taxi_data_quality_ruleset`) evaluates the `clean_trips` table against rules covering fare/distance non-negativity, column completeness, and passenger count range. The Airflow task fails the run if any rule fails, acting as a quality gate before the pipeline is marked successful.

### 5. Analytics (Redshift Spectrum)
A Redshift Serverless workgroup queries the Glue Catalog directly via an external schema, so the silver and gold Parquet tables are queryable with standard SQL — no data duplication into Redshift's native storage.

```sql
CREATE EXTERNAL SCHEMA ny_taxi_external
FROM DATA CATALOG
DATABASE 'ny_taxi_db'
IAM_ROLE 'arn:aws:iam::<account_id>:role/city-data-project-redshift-role'
REGION 'us-east-1';

SELECT * FROM ny_taxi_external.clean_trips LIMIT 10;
```

## Orchestration

A single Airflow DAG (`ny_yellow_taxi_data_pipeline`) runs the full sequence monthly:

```
start → extract_data → transform_data → data_quality_check → end
```

Date boundaries for each run are computed from Airflow's `data_interval_start`/`data_interval_end`, so each scheduled run automatically covers its own calendar month.

## Infrastructure as code

All AWS resources are provisioned with Terraform:
- IAM roles and policies for Glue and Redshift
- Glue Catalog database and tables (silver + gold)
- Glue Data Quality ruleset
- Redshift Serverless namespace and workgroup

```bash
cd terraform/
terraform init
terraform plan
terraform apply
```

## Known limitations

- Each monthly run samples up to a configurable row cap rather than pulling every trip for the month, to keep Glue job runtime and driver memory predictable.
- `revenue_per_min` in the gold layer reflects in-trip time only, not wait time between fares.
- Tipping analysis is restricted to credit card payments, since cash tips aren't captured in the source data.

## Possible next steps

- Add a dbt project on top of Redshift Spectrum for SQL-based gold-layer modeling and testing
- Automate partition repair as its own Airflow task rather than a manual step
- Add row-level lineage via a generated trip identifier
