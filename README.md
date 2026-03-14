# BeejanRide Analytics Modelling Project
BeejanRide is a fast‑growing UK mobility startup operating in 5 cities, offering ride‑hailing, airport transfers, and scheduled corporate rides.
This project implements a scalable, well‑tested, documented, and production‑ready analytics platform using dbt on top of a modern data stack. Bigquery was used as the data warehouse.

## Architectural Diagram

![Architectural Diagram](images/Architectural%20Diagram.gif)

## Data Flow Illustration

### 1. Ingestion
- Source: postgres transactional database
- Tool: Airbyte used to ingest raw data into the data warehouse - Bigquery
- Raw Layer: Raw tables included trips_raw, drivers_raw, riders_raw, payments_raw, cities_raw, driver_status_events_raw. The tables are immutable i.e no modifications are allowed.
The raw layer was the ingested data into the bigquery dataset which was the entries of the transactional database that speaks to the BeejanRide platform.

Some of the decisions made during this workflow step included using incremental-Append for syncing data while using the primary keys on each table as the cursor as against using the Incremental| Append + Deduped which wasn't allowed for a trial account.  An automated sync was also enabled for every 1hr(the lowest that could be gotten for a trial account), to keep the data fresh as much as possible.


#### Generic Tests
Some of the generic tests done in the source.yml file included:
- unique
- not_null
- accepted_values
- relationships

### 2. Staging Layer

This workstep involved applying some cleaning and standardization to the ingested raw data on the data warehouse, which included:
- Renaming columns to snake_case.
- Casting to correct data types.
- Deduplication using primary keys.
- Standardizing timestamps and
- Removing invalid/null primary keys.

The models on the staging layer was build on the source tables by connection to the raw tables in the data warehouse by ensuring that the right parameters(name, database and schema was supplied, as well as the correct table names).
This is a link to the sources documentation from dbt docs

- [dbt docs -- sources](https://docs.getdbt.com/docs/build/sources?version=1.12)

### 3. Intermediate Layer
This was where majority of the heavy-lifting was done to suit the business use cases by enriching the staging data with the necessary business logic. Some of the column enrichments included but not limited to:
- trip_duration_minutes
- driver_lifetime_trips
- rider_lifetime_value
- corporate_trip_flag
- net_revenue calculation
- Fraud indicators: duplicate payments, failed payment on completed trip, extreme surge multiplier (>10).

This stage also involved the use of macros for reusable business logics such as calculating the trip_duration_minutes and net_revenue calculation. I also tweaked the dbt default macro of generate_schema_name to create schemas for staging, intermediate and marts. Macros in Jinja are pieces of code that can be reused multiple times – they are analogous to "functions" in other programming languages, and are extremely useful for repeated logics across multiple models.

Below is a link to the jinja and macros and custom_generate_schema sections on dbt docs website:
- [jinja and macro](https://docs.getdbt.com/docs/build/jinja-macros?version=1.12)
- [custom schemas](https://docs.getdbt.com/docs/build/custom-schemas?version=1.12)

### 4. Marts
This workstep involved organizing the data into a star schema with fact and dimension tables for easy BI consumption by building on the intermediate models.

- Fact Table: facts_trips (trip grain, measures + fraud flags).
- Dimensions: dim_drivers, dim_riders, dim_cities for extra attributes on the entities.

The lineage of the data models created can be found be running the commands:

```

    dbt docs generate

```

Then to display on a browser;

```

    dbt docs serve

```

### Lineage Graph

![Lineage Graph](/images/Lineage.png)

### ERD Diagram

![ERD Diagram](/images/ERD.png)

## Snapshots
Other considerations included having a snapshot to track slowly changing dimensions(SCD) like driver status, vehicle assignments and rating updates. The default dbt SCD type 2 snapshot was used for the drivers on the drivers table which tracks the changes by introducing flags for everytime a change is done without inserting an additional record for the new row, but this was done on an ephemeral materialization so as to first carry out some transformations on the raw drivers table, without materializing it on the data warehouse before a snapshot is applied thereby saving compute cost and size.

Link to the snapshot documentation:
- [snapshots](https://docs.getdbt.com/docs/build/snapshots?version=1.12)


## Design Decisions
During the cause of the project, some design decisions were taken based on personal judgement and what is sensed to serve the business use case properly. Some of which included:
- Fraud Indicators in Fact: stored directly in facts_trips instead of having a separate model for fraud analysis. This was to allow for easier querying and eliminate repetition of steps.
- Single Facts Table: All facts measures consolided at the trip grain.
- Incremental Model: Used incremental models for high volume tables, so as to avoid the compute cost associated with full refresh. This of course reduces the build time in transforming new records by limiting the data that needs to be transformed and invariably reducing compute cost and improving warehouse performance everytime the model is run as against making use of full refresh. However, this comes with the drawback of requiring extra configuration.
- Metadata Governance: Owner and tags applied in marts layer only, keeping staging lean.

## Incremental Models vs Tables(Full refresh)
As previously stated, incremental models are useful for models requiring complex transformations and heavy data. This helps to transform just the new records that are added to the table using the rows that  in your source data that you tell dbt to filter for, inserting them into the target table which is the table that has already been built. Often, the rows you filter for on an incremental run will be the rows in your source data that have been created or updated since the last time dbt ran. As such, on each dbt run, your model gets built incrementally. This is done by adding a 'unique key' to the incremental model configuration. By doing this, the old records in the table are updated since the last run, while new unique records are inserted. In summary, it typically does a merge and insert command on bigquery.

Using an incremental model limits the amount of data that needs to be transformed, vastly reducing the runtime of your transformations. This improves warehouse performance and reduces compute costs. This often come at a cost of extra configurations in the setup and sometimes having to make do with delayed queues of new data.

Full refresh on the other hand is used for a model that has been materialized as a table on the data warehouse and is used to get dbt to rebuild the model again by dropping the table(albeit silently) and creating it again from scratch so as to have new records in the table. Tables are however very quick to query by BI tools as it is more or less static until, but this can take a long time to build especially those involving complex transformations.

Below is a link to the materialization sections on dbt docs:
- [Materializations](https://docs.getdbt.com/docs/build/materializations?version=1.12)
- [Incremental Models](https://docs.getdbt.com/docs/build/incremental-models?version=1.12)

## Custom Tests
Additional data quality checks included building custom tests that speaks to specific business logics such as ensuring there was no negative revenue, completed trips has a valid payment and trip duration minutes for completed trips was always greater than zero minutes. Further guidance on custom tests can be found on the dbt documentation page using the link below:

- [singular data test](https://docs.getdbt.com/docs/build/data-tests?version=1.12)

## Future Improvements
This modelling project was a learning curve for me and not one devoid of areas requiring improvements and future expansion. One that easily comes to mind is having a vehicle dimension table that possesses attributes regarding the vehicle type, licence number, model etc used for a trip -- since a vehicle id is already present in the fact table. It will also make a lot of sense to orchestrate the whole workflow using an orchestrator for a seamless process.

## Sample Analytical Queries
Some of the analytical queries that can be used to get reporting insights from the final serving models includes:

### TOP DRIVERS BY REVENUE
```
    select
        d.driver_id
        , d.city_id
        , d.rating
        , sum(f.net_revenue) as total_driver_revenue
    from {{ ref('facts_trips') }} f
    join {{ ref('dim_drivers') }} d
    on f.driver_id = d.driver_id
    where f.status = 'completed'
    group by d.driver_id, d.city_id, d.rating
    order by total_driver_revenue desc
    limit 10
```

### FRUAD DETECTION INSIGHTS

```
select
    f.trip_id,
    f.city_id,
    f.driver_id,
    f.rider_id,
    f.duplicate_payment_flag,
    f.failed_payment_on_completed_trip,
    f.extreme_surge_flag
from {{ ref('facts_trips') }} f
where f.duplicate_payment_flag = 'Y'
    or f.failed_payment_on_completed_trip = 'Y'
    or f.extreme_surge_flag = 'Y'
```
These could be found on the analyses directory.


### SETUP
**1** Create a project directory on vscode and then create and activate python environment using:

```

    python -m venv .venv

```

activate:

```

    source venv/Scripts/activate

```

**2** While in the environment, install dbt-core and bigquery adapter(or the adapter of any data warehouse you want to use) by running the command:
```

    python -m pip install dbt-core dbt-bigquery

```

**3** Sign in to the [GCP platform](console.cloud.google) and create a new project

**4** Navigate to or search for Bigquery and then create a service account(A service account route was used for authentication in the case because billing is not enabled and it is a free account)

**5** Enable the Big Data Editor and Bigquery User roles for this service account and click on done.

**6** Click on the created service account and navigate to keys, add key-json format. This automaticallly downloads the key as a json file. Copy this and paste in your project directory at the same location where the virtual environment folder is located.

**7** In your bigquery project on gcp, create a dataset with a valid dataset name and a dataset location.

**8** On the bash terminal, with the virtual environment activated run this to initialialize the dbt boostrapper.

```

    dbt init

```

Follow the prompts and provide the right responses such as dbt project name, bigquery for database,service account for authenticatication method, json key file path(just right-click on the json key in your project directory and copy the full file path), ensure you choose the same location as your dataset on bigquery and click on enter button to select default values for some prompts where applicable e.g job execution timeout seconds.
When completed, dbt creates a new dbt directory with some other sub-directories like seeds, macros, models, tests, etc

A .dbt file is also created outside of the parent directory and when navigated into, the cnnection details to the data warehouse can be found. This is editable in case of any corrections that needs to be made.

**9** Testing the dbt connection
Navigate to the created dbt project directory after initialization and run the command:

```

    dbt debug

```

This shows passed when the connection is done right.


### CONTRIBUTIONS
Feel free to make corrections, provide recommendations and other necessary contributions to scale up this project. Best Regards...
