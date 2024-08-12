import logging
import os

from distributed import LocalCluster, Client


def create_client():
    # Get the total number of CPUs available on the machine
    total_cores = os.environ.get('SLURM_CPUS_PER_TASK')
    if total_cores is None:
        total_cores = os.cpu_count()

    # Create a LocalCluster with the calculated number of workers
    cluster = LocalCluster(
        dashboard_address='127.0.0.1:8989',
        n_workers=int(total_cores) - 2,
        threads_per_worker=1,
        memory_limit='4 GiB',
        silence_logs=logging.ERROR
    )

    client = Client(cluster)
    return client
