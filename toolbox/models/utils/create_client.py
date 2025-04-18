
import os

from dask.distributed import LocalCluster, Client

import warnings
import distributed

import logging

from toolbox.utlis.logging import logger


def total_workers():
    total_cores = os.environ.get("SLURM_CPUS_PER_TASK")
    if total_cores is None:
        total_cores = os.cpu_count()
    total_nodes = os.environ.get("SLURM_NTASKS")
    if total_nodes is None:
        total_nodes = 1

    return (int(total_cores) * int(total_nodes)) - 2


def get_cluster_machines(client):
    scheduler_info = client.scheduler_info()
    workers = scheduler_info["workers"]
    machines = set(worker_info["host"] for worker_info in workers.values())
    return machines


def create_client(is_slurm_client: bool):
    # Get the total number of CPUs available on the machine

    logger.info("Creating computation client")

    if is_slurm_client:
        client = Client(
            scheduler_file=os.environ.get("DEEPFRI_PATH") + "/scheduler.json"
        )
        n = total_workers()
        logger.debug("Waiting for {} clients".format(n))
        client.wait_for_workers(n, 300.0)
    else:
        total_cores = os.cpu_count()
        # Create a LocalCluster with the calculated number of workers
        cluster = LocalCluster(
            dashboard_address="0.0.0.0:8989",
            n_workers= 8, # int(0.9 * float(total_cores)),
            threads_per_worker=1,
            memory_limit="100 GiB",
            silence_logs=logging.ERROR,
        )

        client = Client(cluster)
    logger.debug(f"Dashboard link: {client.dashboard_link}")
    logger.debug(f"Workers count: {len(client.scheduler_info()['workers'])}")
    logger.debug(f"Machines: {get_cluster_machines(client)}")

    warnings.simplefilter("ignore", distributed.comm.core.CommClosedError)
    warnings.filterwarnings(
        "ignore",
        message=".*Creating scratch directories is taking a surprisingly long time.*",
    )

    return client
