#!/usr/bin/env python3
import datetime

import httpx
import psycopg2
from prefect import flow, get_run_logger, task


# Task to retrieve petstore inventory or db
@task(name="retrieve_petstore_inventory")
def retrieve_petstore_inventory(base_url: str, path: str, secure: bool):
    logger = get_run_logger()
    if secure:
        url = f"https://{base_url}"
    else:
        url = f"http://{base_url}"
    url = f"{url}{path}"
    response = httpx.get(url, verify=secure)
    response.raise_for_status()
    inventory_stats = response.json()
    logger.info(f"Retrieved inventory stats: {inventory_stats}")
    return inventory_stats


# Task to clean the status date
@task(name="clean_status_date")
def clean_status_date(inventory_stats: dict) -> dict:
    return {
        "sold": int(inventory_stats.get("sold", 0))
        + int(inventory_stats.get("sold ", 0))
        + int(inventory_stats.get("SOLD", 0)),
        "available": int(inventory_stats.get("available", 0))
        + int(inventory_stats.get("avalible", 0))
        + int(inventory_stats.get("availabl", 0))
        + int(inventory_stats.get("available, sold", 0))
        + int(inventory_stats.get("avaliable", 0)),
        "pending": int(inventory_stats.get("pending", 0))
        + int(inventory_stats.get("penidng", 0)),
        "unavailable": int(inventory_stats.get("NOT available", 0)),
    }


@task(name="insert_to_db")
def insert_to_db(
    inventory_stats: dict,
    db_host: str,
    db_user: str,
    db_password: str,
    db_name: str,
):
    # Placeholder for database insertion logic
    with psycopg2.connect(
        host=db_host, user=db_user, password=db_password, dbname=db_name
    ) as conn:
        with conn.cursor() as cursor:
            # Example insertion logic
            cursor.execute(
                """
                INSERT INTO inventory_history (fetch_timestamp, sold, available, pending, unavailable)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    datetime.datetime.now(),
                    inventory_stats.get("sold", 0),
                    inventory_stats.get("available", 0),
                    inventory_stats.get("pending", 0),
                    inventory_stats.get("unavailable", 0),
                ),
            )
            conn.commit()


# Flow to collect petstore inventory
@flow(name="Collect Petstore Inventory")
def collect_petstore_inventory(
    base_url: str = "petstore.swagger.io",
    path: str = "/v2/store/inventory",
    secure: bool = True,
    db_host: str = "localhost",
    db_user: str = "root",
    db_password: str = "root",
    db_name: str = "petstore",
):
    inventory_stats = retrieve_petstore_inventory(base_url, path, secure)
    inventory_stats = clean_status_date(inventory_stats)
    insert_to_db(inventory_stats, db_host, db_user, db_password, db_name)


def main():
    collect_petstore_inventory.serve("petstore-collection-deployment")


if __name__ == "__main__":
    main()
