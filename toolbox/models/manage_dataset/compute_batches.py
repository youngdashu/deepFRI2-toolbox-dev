from typing import List, Any, Callable, Generator, Tuple

from dask.distributed import Client, Semaphore, as_completed, Future


class ComputeBatches:

    def __init__(self, client: Client, run_f, collect_f):
        self.client = client
        self.run_f: Callable[[Any], Future] = run_f
        self.collect_f: Callable[[Any], None] = collect_f

    def compute(self, inputs: Generator[Tuple[Any], Any, None]):
        max_workers = max(len(self.client.nthreads()) // 10, 1)
        semaphore = Semaphore(max_leases=max_workers)

        futures = []
        i = 0

        def collect():
            for batch in as_completed(futures, with_results=True).batches():
                for _, result in batch:
                    self.collect_f(result)
                    semaphore.release()

        while True:
            next_value = next(inputs, None)
            if next_value is None:
                break
            if max_workers > semaphore.get_value():
                semaphore.acquire()
                future = self.run_f(next_value)
                futures.append(future)
            else:
                collect()
                futures.clear()

        collect()
