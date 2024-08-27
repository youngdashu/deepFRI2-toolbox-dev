from typing import Any, Callable, Generator, Tuple

import dask.distributed
from dask.distributed import Client, Semaphore, as_completed, Future, performance_report


class ComputeBatches:

    def __init__(self, client: Client, run_f, collect_f, name: str):
        self.client = client
        self.run_f: Callable[[Any], Future] = run_f
        self.collect_f: Callable[[Any], None] = collect_f
        self.name: str = name

    def compute(self, inputs: Generator[Tuple[Any], Any, None], factor=10):
        max_workers = max(self._workers_num_() // factor, 1)
        semaphore = Semaphore(max_leases=max_workers)

        print(f"Max parallel workers {max_workers}")

        futures = []

        def collect():
            dask.distributed.print("Collecting results")
            count = 0
            for batch in as_completed(futures, with_results=True).batches():
                for _, result in batch:
                    self.collect_f(result)
                    count += 1
                    semaphore.release()
            dask.distributed.print(f"{count} results collected")

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
                    collect()
                    futures.clear()

            collect()

    def _workers_num_(self):
        return len(self.client.scheduler_info()['workers'])