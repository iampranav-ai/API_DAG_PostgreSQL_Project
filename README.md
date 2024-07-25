# API_DAG_PostgreSQL_Project

# Random Jokes Fetcher and Storer

This Airflow DAG fetches random jokes from an API and stores them in a PostgreSQL database.

## Overview

The DAG performs the following tasks:
1. Creates a table in PostgreSQL if it doesn't exist
2. Fetches random jokes from an API for 10 seconds
3. Inserts the fetched jokes into the PostgreSQL database

## DAG Details

- **DAG ID**: `fetch_and_store_random_jokes`
- **Schedule**: Runs every 10 seconds
- **Start Date**: June 20, 2024
- **Catchup**: False
- **Tags**: api, jokes, postgres

## Tasks

1. **create_table**: Creates the `random_joke_api` table in PostgreSQL if it doesn't exist.
2. **fetch_and_store_jokes**: Fetches jokes from the API for 10 seconds and stores them in XCom.
3. **insert_jokes**: Retrieves jokes from XCom and inserts them into the PostgreSQL database.

## Dependencies

- Python 3.x
- Apache Airflow
- requests
- pytz
- PostgreSQL

## Configuration

### Airflow Connection

Set up an Airflow connection with the ID `airflow_connection` for PostgreSQL access.

### API Details

- **API URL**: https://official-joke-api.appspot.com/random_joke

## Database Schema

The `random_joke_api` table has the following structure:

| Column     | Type                     |
|------------|--------------------------|
| id         | SERIAL                   |
| joke_id    | INTEGER                  |
| type       | TEXT                     |
| setup      | TEXT                     |
| punchline  | TEXT                     |
| timestamp  | TIMESTAMP WITH TIME ZONE |

## Usage

1. Ensure Airflow and PostgreSQL are set up and running.
2. Configure the Airflow connection for PostgreSQL.
3. Place the DAG file in your Airflow DAGs folder.
4. The DAG will run automatically according to the schedule.

## Screenshots
![Random_Jokes](https://github.com/user-attachments/assets/8af9d204-c202-4c13-a363-fa711b44d43d)

## Notes
- The DAG fetches jokes for 10 seconds with a 0.5-second delay between each request.
- Timestamps are stored in IST (Indian Standard Time).

## Author

pranavVerma
