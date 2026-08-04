#!/usr/bin/env python3
import httpx
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


# Flow to collect petstore inventory
@flow(name="Collect Petstore Inventory")
def collect_petstore_inventory(
    base_url: str = "petstore.swagger.io",
    path: str = "/v2/store/inventory",
    secure: bool = True,
):
    inventory_stats = retrieve_petstore_inventory(base_url, path, secure)


def main():
    collect_petstore_inventory.serve("petstore-collection-deployment")


if __name__ == "__main__":
    main()
