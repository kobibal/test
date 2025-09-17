import requests
import pandas as pd
import psycopg
from trino.dbapi import connect as trino_connect
from sqlalchemy import create_engine
from datetime import datetime
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence, Union

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
POSTGRES_URL = "postgresql+psycopg://kobi:kobi@localhost:5432/test"
POSTGRES_CONN_STR = "host=localhost port=5432 dbname=test user=kobi password=kobi"

TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "kobi"
TRINO_CATALOG = "postgresql"
TRINO_SCHEMA = "public"

SPACEX_LAUNCHES = "https://api.spacexdata.com/v5/launches"
SPACEX_LATEST = "https://api.spacexdata.com/v5/launches/latest"
DIMS = [
    {
        "table_name": "payloads",
        "url_endpoind": "https://api.spacexdata.com/v4/payloads",
    },
    {
        "table_name": "launchpads",
        "url_endpoind": "https://api.spacexdata.com/v4/launchpads",
    },
]


# API
def get_api_data(url: str, timeout: int = 10) -> Optional[Dict[str, Any]]:
    try:
        logger.info(f"Sending GET request to {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an error for bad status codes
        logger.info("Request successful")
        res = response.json()
        return res

    except requests.exceptions.Timeout:
        logger.error("Request timed out")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
    except ValueError as e:
        logger.error(f"Failed to parse JSON: {e}")

    return None


# Query and data load
def df_to_sql(
    df: pd.DataFrame, table: str, if_exists: str = "append", schema: str = "public")-> None:
    engine = create_engine(
        "postgresql+psycopg://kobi:kobi@localhost:5432/test", future=True
    )
    df.to_sql(
        table,
        engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        chunksize=20_000,
        method="multi",
    )


def trino_query(sql_path: Union[str, Path]) -> pd.DataFrame:
    sql = Path(sql_path).read_text(encoding="utf-8")

    conn = trino_connect(
        host="localhost",
        port=8080,
        user="kobi",
        catalog="postgresql",
        schema="public",
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data=rows, columns=columns)
        return df
    finally:
        conn.close()


def postgres_query(sql_path: Union[str, Path], conn_str: str = POSTGRES_CONN_STR) -> None:
    sql = Path(sql_path).read_text(encoding="utf-8")
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()


# Transformations


def json_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    for col in columns:
        df[col] = df[col].apply(json.dumps)
    return df


def timestamp_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    for col in columns:
        df[col] = (
            df[col]
            .str.replace("Z", "+00:00")
            .apply(lambda x: datetime.fromisoformat(x) if isinstance(x, str) else None)
        )
        # df[col] =  pd.to_datetime(df[col], errors="coerce")
    return df


def dims_prep(dims: Iterable[Dict[str, str]]) -> None:
    for dim in dims:
        dim_data = get_api_data(url=dim["url_endpoind"])
        df = pd.json_normalize(dim_data, sep="_")
        df_to_sql(df, if_exists="replace", table=f"dim_{dim['table_name']}")


# def explode_col(df, col):
#     df = df.copy()
#     df[col] = df[col].str.strip("{}").str.split(",")
#     df = df.explode(col).reset_index(drop=True)
#     return df


# Function for loading at first time, just for getting data in main table
def get_raw_level_data_first_time_load() -> None:
    res = get_api_data(url=SPACEX_LAUNCHES)
    df = pd.json_normalize(res, sep="_")
    df = json_columns(df, columns=["crew", "failures", "cores"])
    df = timestamp_columns(
        df, columns=["static_fire_date_utc", "date_utc", "date_local"]
    )
    df_to_sql(df, if_exists="replace", table="raw_level")


def main() -> None:
    # Create raw_level
    get_raw_level_data_first_time_load()
    logger.info("Successful create raw_data table")

    # Extract - latest
    latest = get_api_data(url=SPACEX_LATEST)
    df_latest = pd.json_normalize(latest, sep="_")
    logger.info("Successful extract latest api data")

    # Transform - latest
    df_latest = json_columns(df_latest, columns=["crew", "failures", "cores"])
    df_latest = timestamp_columns(
        df_latest, columns=["static_fire_date_utc", "date_utc", "date_local"]
    )
    logger.info("Successful transform latest api data")

    # Incremenal load latest launch to raw_data table
    raw_data_ids = trino_query(sql_path="sql/raw_level_incremenal_load.sql")
    if df_latest["id"].values not in raw_data_ids["id"].values:
        df_to_sql(df_latest, table="raw_level_latest")
        logger.info("Successful load incremet data to raw_data table")

    # Dimensions enrichment
    dims_prep(DIMS)
    logger.info("Successful create dimensions tables")

    # Create aggregated table with postgress
    postgres_query(sql_path="sql/aggregated.sql")
    logger.info("Successful create aggregated table")


if __name__ == "__main__":
    main()
