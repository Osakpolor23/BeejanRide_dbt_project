# BeejanRide Analytics Modelling Project
BeejanRide is a fast‑growing UK mobility startup operating in 5 cities, offering ride‑hailing, airport transfers, and scheduled corporate rides.
This project implements a scalable, well‑tested, documented, and production‑ready analytics platform using dbt on top of a modern data stack.

## Data Flow Illustration
### 1. Ingestion
- Source: postgres transactional database
- Tool: Airbyte used to ingest raw data into the data warehouse - Bigquery
- Raw Layer: Raw tables included trips_raw, drivers_raw, riders_raw, payments_raw, cities_raw, driver_status_events_raw. The tables are immutable i.e no modifications are allowed.

Some of the decisions made during this workflow step included using incremental-Append for syncing data while using the primary keys on each table as the cursor as against using the Incremental| Append + Dedupe which wasn't allowed for a trial account.  An automated sync was also enabled for every 1hr(the lowest that could be gotten for a trial account), to keep the data fresh as much as possible

### 2. Staging Layer

This workstep involved applying some cleaning and standardization to the ingested raw data on the data warehouse, which included:
- Renaming columns to snake_case.
- Casting to correct data types.
- Deduplication using primary keys.
- Standardizing timestamps and
- Remove invalid/null primary keys.

### 3. Intermediate Layer
This is where majority of the heavy-lifting was done to suit the business use cases by enriching the staging data with business logic. Some of the column enrichments includes but not limited to:
- trip_duration_minutes
- driver_lifetime_trips
- rider_lifetime_value
- corporate_trip_flag
- net_revenue calculation
- Fraud indicators: duplicate payments, failed payment on completed trip, extreme surge multiplier (>10).

### 4. Marts

