# Snowflake & dbt using Airbnb data

Data source - http://insideairbnb.com/

## Connected dbt to Snowflake
1. Created a new SQL Worksheet in Snowflake
2. Executed SQL: created dbt user, database and schema, set up permissions. 
```sql {#snowflake_setup}
-- Use an admin role
USE ROLE ACCOUNTADMIN;

-- Create the `transform` role
CREATE ROLE IF NOT EXISTS transform;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Create the default warehouse if necessary
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Create the `dbt` user and assign to role
CREATE USER IF NOT EXISTS dbt
  PASSWORD='dbtPassword123'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE='transform'
  DEFAULT_NAMESPACE='AIRBNB.RAW'
  COMMENT='DBT user used for data transformation';
GRANT ROLE transform to USER dbt;

-- Create our database and schemas
CREATE DATABASE IF NOT EXISTS AIRBNB;
CREATE SCHEMA IF NOT EXISTS AIRBNB.RAW;

-- Set up permissions to role `transform`
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE transform; 
GRANT ALL ON DATABASE AIRBNB to ROLE transform;
GRANT ALL ON ALL SCHEMAS IN DATABASE AIRBNB to ROLE transform;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE AIRBNB to ROLE transform;
GRANT ALL ON ALL TABLES IN SCHEMA AIRBNB.RAW to ROLE transform;
GRANT ALL ON FUTURE TABLES IN SCHEMA AIRBNB.RAW to ROLE transform;
```
3. Imported the data
```sql {#snowflake_import}
-- Set up the defaults
USE WAREHOUSE COMPUTE_WH;
USE DATABASE airbnb;
USE SCHEMA RAW;

-- Create our three tables and import the data from S3
CREATE OR REPLACE TABLE raw_listings
                    (id integer,
                     listing_url string,
                     name string,
                     room_type string,
                     minimum_nights integer,
                     host_id integer,
                     price string,
                     created_at datetime,
                     updated_at datetime);
                    
COPY INTO raw_listings (id,
                        listing_url,
                        name,
                        room_type,
                        minimum_nights,
                        host_id,
                        price,
                        created_at,
                        updated_at)
                   from 's3://dbtlearn/listings.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');
                    

CREATE OR REPLACE TABLE raw_reviews
                    (listing_id integer,
                     date datetime,
                     reviewer_name string,
                     comments string,
                     sentiment string);
                    
COPY INTO raw_reviews (listing_id, date, reviewer_name, comments, sentiment)
                   from 's3://dbtlearn/reviews.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');
                    

CREATE OR REPLACE TABLE raw_hosts
                    (id integer,
                     name string,
                     is_superhost string,
                     created_at datetime,
                     updated_at datetime);
                    
COPY INTO raw_hosts (id, name, is_superhost, created_at, updated_at)
                   from 's3://dbtlearn/hosts.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```
## Result
![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/fd79cac9-9411-4603-ba04-8ca81501313a)

 Virtualenv setup, dbt installation
Created project folder using cmd
```
D:
cd da\dbt
mkdir airbnb
cd airbnb
```
Using Python package manager pip installed virtualend (tool that helps manage multiple Python environments in a single machine). It allows to create isolated Python environments, each with its own set of installed packages and dependencies.
```
pip install virtualenv
```
Created and activated virtual environment
```
virtualenv venv
venv\Scripts\activate
```
Result

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/1dbb9199-a080-4853-80e3-a3f654b80094)

Installed dbt
```
pip install dbt-snowflake==1.7.1
```
Initialized the dbt profiles folder 
```
mkdir %userprofile%\.dbt
```
Created dbt project
```
dbt init dbtlearn
```
To complete the connection:
- Selected Snowflake as a database
- Entered credentials (account, username, password, etc.)

To connect:
```
cd dbtlearn
dbt debug
```
Result

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/3c38cf89-147f-45ed-bdfc-d324dd5ede75)

PS From the first try, the connection test failed. I checked the credentials in profiles.yml file. It turned out that the password was entered incorrectly, so I updated it.

 Dbt project structure overview

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/db429e19-0098-42cc-a905-20b27d9223b4)

I deleted everything after "models: dbtlearn:" at the end of dbt_project.yml
Then came to models and deleted the example folder itself.

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/47ebb0ac-478b-48a7-b2d6-8a6892e9f9af)

### Installed and set up dbt extension
In VS code found and installed 'dbt Power User' extension. Used the instructions to set it up.

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/dd0b30e8-b308-4772-8501-7f8d99e213cb)

### Data model

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/a3dc9c72-f80f-4035-8360-08b49699bc82)

## Data flow progress: Staging layer

For now, I have three input tables: raw_hosts, raw_listings and raw_reviews.
The first cleansing step - creating Staging layer.

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/5bdd1c68-17a8-480d-b206-0059dfad34ca)

First I expressed statements in Snowflake (make sure that sql works well). 
Then I integrated them into dbt and made sure that it could execute the statements.

### Raw_listings

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/9ba12d38-8c46-4138-90eb-cdfc16622b9c)

1. I wrote  SQL query in Snowflake that:
- Renames id and name columns to make them more meaningful
- Renames price column to price_str as it has VARCHAR type

```sql
WITH raw_listings as (
    SELECT * FROM AIRBNB.RAW.RAW_LISTINGS
)

SELECT
    id as listing_id,
    name as listing_name,
    listing_url,
    room_type,
    minimum_nights,
    host_id,
    price as price_str,
    created_at,
    updated_at
FROM
    raw_listings
```
2. Opened dbt project. Created new folder in models for src layer

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/6f475418-ea5d-48ed-93fe-8308239bf33f)

3. Created new SQL file and placed SQL statement there.

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/bcb372bc-69e8-4ef9-8e6e-09677ab688f7)

4. In terminal run the query

```
dbt run
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/f0e219bb-bef0-4fa3-bfd1-485691b73eea)

5. Checked the schema in Snowflake

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/f255e14e-0835-48db-9026-8a6c0f8a150e)

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/5b641434-78e1-4af5-b7c2-99dde6d25a7c)

### Raw_reviews

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/183a80a4-6532-434a-964d-ce1bbd06a84d)

1. SQL query to rename the columns date, comments and sentiment
```sql
WITH raw_reviews as (
    SELECT * FROM AIRBNB.RAW.RAW_REVIEWS
)

SELECT
    listing_id,
    date as review_date,
    reviewer_name,
    comments as review_text,
    sentiment as review_sentiment
FROM
    raw_reviews
```
3-5. Completed as for raw_listings

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/27b86535-134e-4a68-92b7-13767c7f3fe1)

### Raw_hosts

1. SQL query to rename the columns id and host_name
```sql
WITH raw_hosts as (
    SELECT * FROM AIRBNB.RAW.RAW_HOSTS
)

SELECT
    id as host_id,
    name as host_name,
    is_superhost,
    created_at,
    updated_at
    
FROM
    raw_hosts
```
3-5. Completed as for raw_listings

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/705b174f-291a-442c-b196-9c8ea3ad7ddd)

## Data flow progress: Core layer

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/d96e86e1-80c9-4beb-9562-db70437e1b1c)

### Dim_listings_cleansed

1. Created new folder 'Dim' in models and new file 'dim_listings_cleansed.sql'

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/439d7a04-127b-478e-9a3c-660acdbf155f)


2. Created and run SQL query

```sql
WITH src_listings as (
    SELECT * FROM {{ ref('src_listings') }}
)
SELECT
    listing_id,
    listing_name,
    room_type,
    case
        when minimum_nights = 0
        then 1
        else minimum_nights
    end as minimum_nights,
    host_id,
    replace(
        price_str,
        '$'
    ) :: number(
        10, 2
    ) as price,
    created_at,
    updated_at    
FROM
    src_listings
```
### Dim_hosts_cleansed
1. Created new file in folder 'dim' - 'dim_hohsts_cleansed.sql'
2. Created and run SQL query

```sql
WITH src_hosts as (
    SELECT * FROM {{ref ('src_hosts')}}
)

SELECT
    host_id,
    nvl(host_name, 'Anonymous') as host_name,
    is_superhost,
    created_at,
    updated_at
FROM    
    src_hosts
```
PS Set up view materialization as default for this project: in dbt_project.yml added a row at the end. Then for dim set default materialization 'table' (because they will be accessed repeatedly) 
```
#models:
#  dbtlearn:
    +materialized: view
    dim:
      +materialized: table
```
Executed dbt run to check:

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/308099ed-695a-4199-9002-b2fcb3244335)

### Dim_reviews_cleansed
For reviews I decided to create an incremental view (there can be appends to this table)
1. Created new file in folder 'dim' -  fct_reviews.sql
2. Created and run SQL (with jinja function 'config'), where defined the condition of increment:
```sql
{{
    config(
    materialized = 'incremental',
    on_schema_change = 'fail'
    )
}}

WITH src_reviews as (
    SELECT * FROM {{ ref('src_reviews')}}
)
SELECT *
FROM src_reviews
WHERE review_text is not null

{% if is_incrmental() %}
    and review_date > (select max(review_date) from {{ this }})
{% endif %}
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/d0664d0c-92dd-4d03-b901-e7090df100f7)

3.1 Simulated adding new review to raw_reviews
```sql
INSERT INTO
    AIRBNB.RAW.RAW_REVIEWS
VALUES
    (3176, 
    current_timestamp(),
    'Zoltan',
    'excellent stay!',
    'positive');
```
3.2 Executed dbt run and checed fct_reviews in Snowflake
```sql
SELECT
    *
FROM
    AIRBNB.DEV.FCT_REVIEWS
ORDER BY
    review_date desc
LIMIT 3
```
![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/70b348c5-baf6-490e-9d63-3da1d370dc2a)

PS to rebuild incremental table run
```
dbt run --full-refresh
```
### Dim_listings_with_hosts
1.  Created new file in folder 'dim' - dim_listings-W-hosts.sql
2.  Created and run SQL query
```sql
WITH
l as (
    SELECT
        *
    FROM
        {{ ref('dim_listings_cleansed') }}
),
h as (
    SELECT * 
    FROM {{ ref('dim_hosts_cleansed') }}
)

SELECT 
    l.listing_id,
    l.listing_name,
    l.room_type,
    l.minimum_nights,
    l.price,
    l.host_id,
    h.host_name,
    h.is_superhost as host_is_superhost,
    l.created_at,
    greatest(l.updated_at, h.updated_at) as updated_at
FROM
    l
LEFT JOIN 
    h 
    on (h.host_id = l.host_id)
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/8f6ea782-6813-41f6-81bc-ea8f6175b4d8)

3. Cleaned up materializations
- I don't need any materialization in source level.
Added to dbt_project.yml:
```
models:
  dbtlearn:
    +materialized: view
    dim:
      +materialized: table
    src:
      +materialized: ephemeral
```
Dropped views in Snowflake
```
DROP VIEW AIRBNB.DEV.SRC_HOSTS;
DROP VIEW AIRBNB.DEV.SRC_LISTINGS;
DROP VIEW AIRBNB.DEV.SRC_REVIEWS;
```
Executed dbt run
Result - no views in Snowflake

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/18cae0dc-6c18-4d37-9a1e-f0169acd6b9f)

To find the queries - in terminal:
```
code target/run/dbtlearn/models/dim/dim_listings_cleansed.sql
```
- Shifted dim_listings_cleansed and dim_hosts_cleansed to view materialization (since I have dimension table dim_listings_w_host) by adding jinjia config function in sql files
```sql
{{
    config(
    materialized = 'view'
    )
}}
```

## Seeds and sources

Seeds:
- Small datasets, live ib seeds folder in .csv format
- You can upload them to dwh from dbt

I uploaded [this file](seed_full_moon_dates.csv) to seeds folder. Question to be answered: Is it true that a full moon has a negative affection on sleep and mood?

Imported that file to Snowflake by running dbt seed.

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/cb1707ae-5842-4734-914f-0201b6dacd61)

Created mart layer (which will be accessible for BI tools) in models folder. Created file 'mart_fullmoon_reviews.sql'

Executed SQL 
```sql
{{ config(
  materialized = 'table',
) }}

WITH fct_reviews as (
    SELECT * FROM {{ ref('fct_reviews') }}
),
full_moon_dates as (
    SELECT * FROM {{ ref('seed_full_moon_dates') }}
)

SELECT
    r.*,
    case
        when fm.full_moon_date is null 
        then 'not full moon'
        else 'full moon'
    end as is_full_moon
FROM
    fct_reviews
    r
LEFT JOIN 
    full_moon_dates
    fm
    on (to_date(r.review_date) = dateadd(DAY, 1, fm.full_moon_date))
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/51209e87-43f6-48c1-9a67-be52c7cdd002)

Sources:
- provide a convenient way to manage connections to external data systems
- you can check the data freshness

I wanted to shift src files to sources. To do this, I created new .yml file in models table

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/c4de7249-b385-4ab4-87a2-2878597b50d9)

Added extra abstraction on top of raw layer. Can refer to raw tables as source tables.
```
version: 2

sources:
  - name: airbnb
    schema: raw
    tables:
      - name: listings
        identifier: raw_listings

      - name: hosts
        identifier: raw_hosts

      - name: reviews
        identifier: raw_reviews
```
Then changed sources in src_listings, src_hosts, srs_reviews
```sql
WITH raw_listings as (
    SELECT * FROM {{ source('airbnb', 'listings') }}
)
```

Checked if everything works well by running dbt compile

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/d49cb051-924c-4101-9987-3d7901f53ea8)

Added in sources.yml when I should be alerted if loading false
```
version: 2

sources:
  - name: airbnb
    schema: raw
    tables:
      - name: listings
        identifier: raw_listings

      - name: hosts
        identifier: raw_hosts

      - name: reviews
        identifier: raw_reviews
        loaded_at_field: date
        freshness:
          warn_after: {count: 1, period: hour}
          error_after: {count: 24, period: hour}
```
Executed dbt source freshness. I received one warning, because the last updated review table was more than an hour ago

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/b561ea1c-131e-4dc6-8d85-d71b732f561e)

## Snapshot table creation
Snapshots are used to store slowly changing dimensions. I made to snapshot tables: on top of raw_listings and raw_hosts.

1. In snapshots folder created new file 'scd_raw_listings.sql'. Added a query
```sql
{% snapshot scd_raw_listings %}

{{
    config(
        target_schema='dev',
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ source('airbnb', 'listings') }}

{% endsnapshot %}
```
2. Executed dbt snapshot, Snapshot was created successfully
3. Updated for id = 3176 in raw_listings
```slq
UPDATE
    AIRBNB.RAW.RAW_LISTINGS
SET
    minimum_nights=30,
    updated_at=current_timestamp()
WHERE 
    id = 3176;
```
4. Reexecuted dbt snapshot. Completed successfully
5. Checked in snowflake
```sql
SELECT
    id,
    minimum_nights,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM
    AIRBNB.DEV.SCD_RAW_LISTINGS
WHERE
    id=3176
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/749de07d-0eed-4fcb-be9b-e96ff9c7875f)

## Tests

### Generic test

I executed dbt build-in generic tests on dim_listings_cleansed table
1. Created new file in models folder 'schema.yml' 
2. Placed there testing conditions
```
version: 2

models:
  - name: dim_listings_cleansed
    columns:

     - name: listing_id
       tests:
         - unique
         - not_null
```
3. Executed with dbt test

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/b7d101b1-c628-4b9e-9f1e-a552c2ba32ca)

4. Added other tests to the file
```
version: 2

models:
  - name: dim_listings_cleansed
    columns:

     - name: listing_id
       tests:
         - unique
         - not_null

     - name: host_id
       tests:
         - not_null
         - relationships:
             to: ref('dim_hosts_cleansed')
             field: host_id

     - name: room_type
       tests:
         - accepted_values:
             values: ['Entire home/apt',
                      'Private room',
                      'Shared room',
                      'Hotel room']
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/9830b969-fcdd-4e1d-b027-d3363210c295)

### Singular test

#### Minimum nights
1. created new file in tests folder 'dim_listings_minimum_nights.sql'.
2. Created a query to check if there are any rows with minimum nights < 1 (singular test is successful when returns 0)
```sql
SELECT
    *
FROM
    {{ ref('dim_listings_cleansed') }}
WHERE 
    minimum_nights < 1
LIMIT 
    10
```
3. Executed dbt test

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/f9c78a6e-3a08-42b5-a837-c1cb81e3bc6c)

#### Created at

1. Created new file 'consistent_created_at.sql'
2. Created a query to check that there is no review date before the listing was created.

```sql
SELECT  
    *
FROM
    {{ ref("dim_listings_cleansed") }} l
INNER JOIN
    {{ ref("fct_reviews") }} r
    using(listing_id)
WHERE 
    r.review_date <= l.created_at
```

3. Executed dbt test

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/48c95d82-a522-4a58-978c-bac8410dfbf8)

### Custom generic test

First I wanted to create singular test based on macro
1. Created macro (jinja template) in macros table to check all columns for null in models. File name 'no_nulls_in_columns.sql'
2. Created query
```sql
{% macro no_nulls_in_columns(model) %}
    SELECT 
        * 
    FROM 
        {{ model }} 
    WHERE
        {% for col in adapter.get_columns_in_relation(model) -%}
            {{ col.column }} IS NULL OR
        {% endfor %}
        FALSE
{% endmacro %}
```
3. Created test file in tests folder 'no_nulls_in_dim_listings.sql'
```sql
{{ no_nulls_in_columns(ref("dim_listings_cleansed")) }}
```
4. Executed all tests for dim_lising_cleansed using
```
dbt test --select dim_listing_cleansed
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/0954b048-1dfe-45ff-9bb0-91e8370f692e)

To create generic test based on macro
1. Created new file in macros folder 'positive_value.sql'
2. I used minimum nights singular test as a base and created sql query
```sql
{% test positive_value(model, column_name) %}
SELECT
    *
FROM
    {{ model }}
WHERE
    {{ column_name}} < 1
{% endtest %}
```
3. In models > schema.yml added new generic test
```
version: 2

models:
  - name: dim_listings_cleansed
    columns:

     - name: listing_id
       tests:
         - unique
         - not_null

     - name: host_id
       tests:
         - not_null
         - relationships:
             to: ref('dim_hosts_cleansed')
             field: host_id

     - name: room_type
       tests:
         - accepted_values:
             values: ['Entire home/apt',
                      'Private room',
                      'Shared room',
                      'Hotel room']

     - name: minimum_nights
       tests:
         - positive_value
```
4. Executed dbt test

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/f3104313-5385-4f56-b2a2-b4bbb1cb1d10)

To add dbt packages to the project:
1. Opened hub.getdbt.com > dbt_units
2. created new file in project 'packages.yml'
3. Copied and pasted installation definition
```
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```
4. To install run dbt deps

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/4df958b2-e209-4470-a3ee-0cc63d47c26a)

I wanted to use the specific function - surrogate_key
1. In models > fct > fct_reviews.sql added column with unique key using generate_surrogate_key
```sql
{{
    config(
    materialized = 'incremental',
    on_schema_change = 'fail'
    )
}}

WITH src_reviews as (
    SELECT * FROM {{ ref('src_reviews')}}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }} as review_id,
    *
FROM src_reviews
WHERE review_text is not null

{% if is_incremental() %}
    and review_date > (select max(review_date) from {{ this }})
{% endif %}
```
2. Added var in dbt_project.yml
```

# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: 'dbtlearn'
version: '1.0.0'
config-version: 2

vars:
  surrogate_key_treat_nulls_as_empty_strings: true #turn on legacy behaviour

# This setting configures which "profile" dbt uses for this project.
profile: 'dbtlearn'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the example/
# directory as views. These settings can be overridden in the individual model
# files using the `{{ config(...) }}` macro.
models:
  dbtlearn:
    +materialized: view
    dim:
      +materialized: table
    src:
      +materialized: ephemeral
```
3. dbt run --full-refresh

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/1f33cbd5-b4bb-4ca4-a99e-fcee8be46b4e)

## Dosumentation
Added documentation ('description:') to schema.yml file
```yaml
version: 2

models:
  - name: dim_listings_cleansed
    description: Cleansed table which contains Airbnb listings.
    columns:

     - name: listing_id
       desciption: Primary key for the listing
       tests:
         - unique
         - not_null

     - name: host_id
       desciption: The hosts's id. References the host table.
       tests:
         - not_null
         - relationships:
             to: ref('dim_hosts_cleansed')
             field: host_id

     - name: room_type
       description: Type of the apartment / room
       tests:
         - accepted_values:
             values: ['Entire home/apt',
                      'Private room',
                      'Shared room',
                      'Hotel room']

     - name: minimum_nights
       tests:
         - positive_value

```
Using command dbt docs found the .json file with documentation

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/285ac247-0ebf-4a2c-b0f2-3735f228d3c3)

Using dbt docs serve command created html page for my project

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/6bfee349-43d8-4971-aea5-39e6a3afb7e6)

In models folder created new file 'docs.md' for focumentation

Placed comment there
```
{% docs dim_listing_cleansed__minimum_nights %}
Minimum number of nights required to rent this property. 

Keep in mind that old listings might have `minimum_nights` set to 0 in the source tables. Our cleansing algorithm updates this to `1`.

{% enddocs %}
```

In schema.yml file referenced to my md. file in models -name:minimum_nights
```yaml
     - name: minimum_nights
       description: '{{ doc("dim_listing_cleansed__minimum_nights") }}'
       tests:
         - positive_value
```

In terminal
```
dbt docs generate

dbt docs serve
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/c8bc7d62-a5b3-4041-9c14-2e3f2aedf6b3)

### Redesign an overview page
Created new file in models 'overview.md'
```
{% docs __overview__ %}
# Airbnb pipeline

Hey, welcome to our Airbnb pipeline documentation!

Here is the schema of our input data:
![input schema](https://dbtlearn.s3.us-east-2.amazonaws.com/input_schema.png)

{% enddocs %}
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/dc351a4f-3402-4f2b-b449-7e98b342b84c)

Created new folder 'assets' to store any files there.
in dbt_project.yml file added row to identify this folder in project
```yaml
asset-paths: ["assets"]
```
Placed the image with scema to assets folder

Replaced link in overview.md to (assets/input_schema.png)

Checked using
```
dbt docs generate
dbt docs serve
```
Then opened Data Flow DAG (to see the full flow of data in the project)

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/4077caf4-87d7-48c5-98db-c5468f080e61)

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/296e35db-1ea4-4edc-ac26-8d98dd49a301)

Added description for dim_hosts_cleansed model into schema.yml file
```yaml
version: 2

models:
  - name: dim_listings_cleansed
    description: Cleansed table which contains Airbnb listings.
    columns:
      
      - name: listing_id
        description: Primary key for the listing
        tests:
          - unique
          - not_null
        
      - name: host_id
        description: The hosts's id. References the host table.
        tests:
          - not_null
          - relationships:
              to: ref('dim_hosts_cleansed')
              field: host_id

      - name: room_type
        description: Type of the apartment / room
        tests:
          - accepted_values:
              values: ['Entire home/apt', 'Private room', 'Shared room', 'Hotel room']

      - name: minimum_nights
        description: '{{ doc("dim_listing_cleansed__minimum_nights") }}'
        tests:
          - positive_value

  - name: dim_hosts_cleansed
    description: Cleansed table which contains Airbnb hosts.
    columns:
      - name: host_id
        description: primary key
        tests:
          - not_null
          - unique
      
      - name: host_name
        description: Name of hosts
        tests:
          - not_null
      
      - name: is_superhost
        description: Contains boolean values whether the host is superhost. t for True and f for False. 
        tests:
          - accepted_values:
              values: ['t', 'f']
  
  - name: fct_reviews
    columns:
      - name: listing_id
        tests:
          - relationships:
              to: ref('dim_listings_cleansed')
              field: listing_id

      - name: reviewer_name
        tests:
          - not_null
      
      - name: review_sentiment
        tests:
          - accepted_values:
              values: ['positive', 'neutral', 'negative']
```

## Analyses
In analyses folder created new sql file full_moon_no_sleep.sql
This means that I don't want to create model on top on this query.
```sql
WITH mart_fullmoon_reviews as (
    SELECT
        *
    FROM
        {{ ref('mart_fullmoon_reviews') }}
)

SELECT  
    is_full_moon,
    review_sentiment,
    count(*) as review
FROM    
    mart_fullmoon_reviews
GROUP BY
    is_full_moon,
    review_sentiment
ORDER BY 
    is_full_moon,
    review_sentiment
```
I can compile this file through terminal
```
dbt compile
cd target\compiled\dbtlearn\analyses
more full_moon_no_sleep.sql 
```

Copied and pasted query into snowflake

```sql
WITH mart_fullmoon_reviews as (
    SELECT
        *
    FROM
        AIRBNB.DEV.mart_fullmoon_reviews
)

SELECT
    is_full_moon,
    review_sentiment,
    count(*) as review
FROM
    mart_fullmoon_reviews
GROUP BY
    is_full_moon,
    review_sentiment
ORDER BY
    is_full_moon,
    review_sentiment
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/791a5cf9-c281-4b9d-9059-dc5fb923dfe8)

### Building dashboard

Create the REPORTER role and PRESET user in Snowflake

```sql
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS REPORTER;
CREATE USER IF NOT EXISTS PRESET
 PASSWORD='presetPassword123'
 LOGIN_NAME='preset'
 MUST_CHANGE_PASSWORD=FALSE
 DEFAULT_WAREHOUSE='COMPUTE_WH'
 DEFAULT_ROLE='REPORTER'
 DEFAULT_NAMESPACE='AIRBNB.DEV'
 COMMENT='Preset user for creating reports';

GRANT ROLE REPORTER TO USER PRESET;
GRANT ROLE REPORTER TO ROLE ACCOUNTADMIN;
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE REPORTER;
GRANT USAGE ON DATABASE AIRBNB TO ROLE REPORTER;
GRANT USAGE ON SCHEMA AIRBNB.DEV TO ROLE REPORTER;

-- We don't want to grant select rights here; we'll do this through hooks:
-- GRANT SELECT ON ALL TABLES IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;
-- GRANT SELECT ON FUTURE VIEWS IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;
```

Switched to reporter role

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/d974b8c4-b89b-4744-806d-a8baef28a0a5)

Added hook to permit reporter see models role in dbt_project.yml
```yaml
models:
  dbtlearn:
    +materialized: view
    +post_hook:
      - "GRANT SELECT ON {{ this }} TO ROLE REPORTER"
    dim:
      +materialized: table
    src:
      +materialized: ephemeral
```

Registered to Presset, entered all necessary credentials and created new dataset

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/d018fa34-3977-40e7-8a3d-dcab43064013)

Created new chart and placed it on a [dashboard](https://ade0514b.us2a.app.preset.io/superset/dashboard/p/JMzy4gBKYd3/)

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/cb976f76-5096-4417-a4dd-c2e7a20d230b)

In models created new file 'dashboards.yml'
```yaml
version: 2

exposures:
  - name: Executive Dashboard
    type: dashboard
    maturity: low
    url: https://ade0514b.us2a.app.preset.io/superset/dashboard/p/JMzy4gBKYd3/
    description: Executive Dashboard about Airbnb listings and hosts
      

    depends_on:
      - ref('dim_listings_w_hosts')
      - ref('mart_fullmoon_reviews')

    owner:
      name: Hanna S
      email: hanna.stselmashok@gmail.com
```

# Advanced dbt
Added package for running tests to packages.yml
```yaml
  - package: calogica/dbt_expectations
    version: 0.10.1
```
Run dbt deps to install the package

## Expect the number of rows in a model match another model.

Added test to schema.yml
```yaml
  - name: dim_listings_w_hosts
    tests:
      - dbt_expectations.expect_table_row_count_to_equal_other_table:
          compare_model: source('airbnb', 'listings')
```
Executed dbt test --select dim_listingsdim_listings_w_hosts

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/5cae665e-ba13-49ef-b70f-faa41571d04a)

## Check price is between specific numbers

Added test to schema.yml (under dim_listings_w_hosts)

```yaml
    columns:
      - name: price
        tests:
          - dbt_expectations.expect_column_quantile_values_to_be_between:
              quantile: .99
              min_value: 50
              max_value: 500
```
dbt test --select dim_listings_w_hosts

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/6ad9a004-23f3-4e76-86a6-c2a6ea5ab473)

## Implementation test warnings for external items

Added test to schema.yml (under dim_listings_w_hosts, price column)

```yaml
          - dbt_expectations.expect_column_max_to_be_between:
              max_value: 500
```

The test failed

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/1b497146-2097-4aa7-ba6e-891d99b7deeb)


Checked the dataset

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/ef1848d4-2906-4e71-91a6-bd9ceb4692ca)


Changed error to warning in schema.yml test

```yaml
              config:
                severity: warn
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/59ab3f89-c8ec-4d81-a475-2d91b0dbd12a)


## Validating column types

Added test in schema.yml (under dim_listings_w_hosts, price column)
```yaml
          - dbt_expectations.expect_column_values_to_be_of_type:
              column_type: number
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/941e8563-a2fd-49f3-bda3-1d8fc041f1fd)

## Monitoring categorical variables in the source data

Added to sources.yml under listings

```yaml
        columns:
          -name: room_type
            tests:
              - dbt_expectations.expect_column_distinct_count_to_equal:
                  value: 4
```

dbt test --select source:airbnb.listings

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/d331e0ee-26d6-4be3-a7fe-a559c746f3b6)

## Debugging test

Added to sources.yml under listings

```yaml
          - name: price
            tests:
              - dbt_expectations.expect_column_values_to_match_regex:
                  regex: "^\\$[0-9][0-9\\.]+$"
```
dbt test --select source:airbnb.listings

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/d326d3b7-68c3-41ca-bf15-93493cc7c5c8)

dbt --debug test --select source:airbnb.listings

To open file with sql query for this test (in terminal code target\compiled\dbtlearn\models\sources.yml\dbt_expectations_source_expect_a60b59a84fbc4577a11df360c50013bb.sql)

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/a1790a09-521c-4fb5-bd18-d93108084e3f)

Run this query in snowflake

```sql
with grouped_expression as (
    select
        regexp_instr(price, '^\$[0-9][0-9\.]+$', 1, 1, 0, '') > 0 as expression
    from 
        AIRBNB.RAW.RAW_LISTINGS
),
validation_errors as (
    select
        *
    from
        grouped_expression
    where
        not(expression = true)
)
select *
from validation_errors
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/3c6d8fee-3dbe-438d-8b6b-b65a791885aa)

I decided to ease the expression and for start check that the values in price column start with dollar sign

```sql
select
    price,
    regexp_instr(price, '^\$') > 0 as expression
from 
    AIRBNB.RAW.RAW_LISTINGS
```

The expressions are still false

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/2b78691c-6711-43c9-955f-9cba876da546)

It seemed that the problem was in double escaping. 

```sql
select
    price,
    regexp_instr(price, '^\\$') > 0 as expression
from 
    AIRBNB.RAW.RAW_LISTINGS
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/4fbd552b-0c2c-4302-b3a8-496a006917ea)

Corrected test in sources.yml (4 backslashes)

```yaml
          - name: price
            tests:
              - dbt_expectations.expect_column_values_to_match_regex:
                  regex: "^\\\\$[0-9][0-9\\\\.]+$"
```

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/6d883266-c69d-40f3-8fd2-be31370356bc)

