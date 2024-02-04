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
### Result
![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/fd79cac9-9411-4603-ba04-8ca81501313a)

## Virtualenv setup, dbt installation
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
cd debtlearn
dbt debug
```
Result

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/3c38cf89-147f-45ed-bdfc-d324dd5ede75)

PS From the fisrtd try, the connection test failed. I checked the credentials in profiles.yml file. It turned out that the password was entered incorrectly, so I updated it.

## Dbt project structure overview

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/db429e19-0098-42cc-a905-20b27d9223b4)

I deleted everything after "models: dbtlearn:" at the end of dbt_project.yml
Then came to models and deleted the example folder itself.

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/47ebb0ac-478b-48a7-b2d6-8a6892e9f9af)

##Installed and set up dbt extension
In VS code found and installed 'dbt Power User' extension. Used the instructions to set it up.

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/dd0b30e8-b308-4772-8501-7f8d99e213cb)

##Data model

![image](https://github.com/HannaStselmashok/snowflake_dbt/assets/99286647/a3dc9c72-f80f-4035-8360-08b49699bc82)

