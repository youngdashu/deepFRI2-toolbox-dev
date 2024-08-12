import logging
import os

from distributed import LocalCluster, Client


def create_client():
    # Get the total number of CPUs available on the machine
    total_cores = os.cpu_count()

    # Create a LocalCluster with the calculated number of workers
    cluster = LocalCluster(
        dashboard_address='127.0.0.1:8786',
        n_workers=total_cores,
        threads_per_worker=1,
        memory_limit='3.5 GiB',
        silence_logs=logging.ERROR
    )

    client = Client(cluster)
    return client
