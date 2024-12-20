import threading
import time
from itertools import cycle
from typing import Any, Callable, Generator, Tuple

import dask.distributed
from dask.distributed import Client, Semaphore, as_completed, Future, performance_report
from distributed import Variable

from toolbox.models.utils.create_client import total_workers, get_cluster_machines


class ComputeBatches:

    def __init__(self, client: Client, run_f, collect_f, name: str):
        self.client = client
        self.run_f: Callable[[Any, str], Future] = run_f
        self.collect_f: Callable[[Any], None] = collect_f
        self.name: str = name

    def compute(self, inputs: Generator[Tuple[Any], Any, None], factor=10):

        machines = get_cluster_machines(self.client)
        machines_c = cycle(machines)

        max_workers = max(self._workers_num_() // factor, 1)
        sem_name = "sem" + self.name
        semaphore = Semaphore(max_leases=max_workers, name=sem_name)

        print(f"Max parallel workers {max_workers}")

        var = Variable('stopping-criterion')
        var.set(False)

        i = 1
        name = f"report_{self.name}_{self._workers_num_()}_{factor}"
        # with performance_report(filename=f"{name}.html"):

        ac = as_completed([], with_results=True)

        collect_thread = threading.Thread(target=collect, args=(ac, self.collect_f, semaphore))
        collect_thread.start()

        while True:
            semaphore.acquire()

            next_value = next(inputs, None)
            if next_value is None:
                break

            print(i)
            i += 1
            future = self.run_f(next_value, next(machines_c))
            ac.add(future)

        var.set(True)

        collect_thread.join()

    def _workers_num_(self):
        return total_workers()


def collect(ac: as_completed, collect_f, semaphore: Semaphore):
    dask.distributed.print("Collecting results")
    count = 0
    total_time = 0
    stop_var = Variable('stopping-criterion')
    while True:

        while ac.is_empty() and not stop_var.get():
            if ac.is_empty() and stop_var.get():
                print("Collect results time:", total_time)
                return
            time.sleep(1)

        if ac.is_empty() and stop_var.get():
            print("Collect results time:", total_time)
            return

        future_c, result = next(ac)
        start_time = time.time()
        collect_f(result)
        end_time = time.time()
        total_time += end_time - start_time
        del future_c
        dask.distributed.print("Collected {}".format(count))
        count += 1
        semaphore.release()

        if ac.is_empty() and stop_var.get():
            print("Collect results time:", total_time)
            return
