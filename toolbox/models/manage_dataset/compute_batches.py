from typing import Any, Callable, Generator, Tuple

import dask.distributed
from dask.distributed import Client, Semaphore, as_completed, Future, performance_report
from distributed import Variable

from toolbox.models.utils.create_client import total_workers


class ComputeBatches:

    def __init__(self, client: Client, run_f, collect_f, name: str):
        self.client = client
        self.run_f: Callable[[Any], Future] = run_f
        self.collect_f: Callable[[Any], None] = collect_f
        self.name: str = name

    def compute2(self, inputs: Generator[Tuple[Any], Any, None], factor=10):
        max_workers = max(self._workers_num_() // factor, 1)
        semaphore = Semaphore(max_leases=max_workers)

        print(f"Max parallel workers {max_workers}")

        futures = []

        def collect(fs):
            dask.distributed.print("Collecting results")
            count = 0
            for batch in as_completed(fs, with_results=True).batches():
                for _, result in batch:
                    self.collect_f(result)
                    count += 1
                    semaphore.release()
            dask.distributed.print(f"{count} results collected")
            fs.clear()

        i = 1
        name = f"report_{self.name}_{self._workers_num_()}_{factor}"
        with performance_report(filename=f"{name}.html"):

            while True:
                if max_workers > semaphore.get_value():

                    next_value = next(inputs, None)
                    if next_value is None:
                        break

                    semaphore.acquire()
                    print(i)
                    i += 1
                    future = self.run_f(next_value)
                    futures.append(future)
                else:
                    collect(futures)
                    # futures.clear()

            collect()

    def compute(self, inputs: Generator[Tuple[Any], Any, None], factor=10):
        max_workers = max(self._workers_num_() // factor, 1)
        semaphore = Semaphore(max_leases=max_workers)

        print(f"Max parallel workers {max_workers}")

        var = Variable('stopping-criterion')
        var.set(False)

        i = 1
        name = f"report_{self.name}_{self._workers_num_()}_{factor}"
        with performance_report(filename=f"{name}.html"):

            ac = as_completed([], with_results=True)

            collect_job = self.client.submit(collect, (ac, self.collect_f, semaphore))

            while True:
                semaphore.acquire()

                next_value = next(inputs, None)
                if next_value is None:
                    break

                print(i)
                i += 1
                future = self.run_f(next_value)
                ac.add(future)

            collect_job.result()

    def _workers_num_(self):
        return total_workers()


def collect(ac: as_completed, collect_f, semaphore: Semaphore):
    dask.distributed.print("Collecting results")
    count = 0
    stop_var = Variable('stopping-criterion')

    while True:
        future_c, result = next(ac)
        collect_f(result)
        dask.distributed.print("Collected {}".format(count))
        count += 1
        semaphore.release()

        if ac.is_empty() and stop_var.get():
            break
