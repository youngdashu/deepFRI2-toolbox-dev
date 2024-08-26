import logging
import os

from dask.distributed import LocalCluster, Client

import warnings
import distributed


def create_client(is_slurm_client: bool):
    # Get the total number of CPUs available on the machine

    if is_slurm_client:
        client = Client(scheduler_file='./scheduler.json')
    else:

        total_cores = os.environ.get('SLURM_CPUS_PER_TASK')
        if total_cores is None:
            total_cores = os.cpu_count()

        # Create a LocalCluster with the calculated number of workers
        cluster = LocalCluster(
            dashboard_address='0.0.0.0:8989',
            n_workers=int(total_cores) - 2,
            threads_per_worker=1,
            memory_limit='16 GiB',
            silence_logs=logging.ERROR
        )

        client = Client(cluster)
    print(client.dashboard_link)

    warnings.simplefilter("ignore", distributed.comm.core.CommClosedError)
    warnings.filterwarnings("ignore", message=".*Creating scratch directories is taking a surprisingly long time.*")

    return client
