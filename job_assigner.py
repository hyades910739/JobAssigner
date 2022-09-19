import operator
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Hashable, List


class JobAssigner:
    "A decorator to assign and count job to different workers."

    def __init__(
        self,
        workers: List[Hashable],
        max_job_per_worker: int,
        worker_arg_name: str,
        if_no_id: str = "raise",
        waiting_time: int = 10,
    ):
        """
        Args:
            workers (List[Hashable]): the name of the workers, the worker name will pass to fun as value for parameter 'worker_arg_name'
            max_job_per_worker (int): the maximum job a worker can run simultaneously.
            worker_arg_name (str): the argument name for function to pass worker name.
            if_no_id (str): Action when there's no worker available. If raise, raise ValueError if wait,
                            keep waiting and try to get id every `waiting_time` second. Defaults to 'raise'.
            waiting_time (int): Waiting time to call `get_worker` again when there's no worker available.
                                This parameter only works when `if_no_id='wait'`. Defaults to 10.
        """
        #
        assert len(workers) > 0
        assert waiting_time > 0
        assert if_no_id in ("wait", "raise")

        self.job_counter = {k: 0 for k in workers}
        self.max_job_per_worker = max_job_per_worker
        self.waiting_time = waiting_time
        self.if_no_id = if_no_id
        self.worker_arg_name = worker_arg_name

    def __call__(self, fun: Callable) -> Callable:
        def inner(**kwargs):
            worker_id = self.get_worker()
            self.job_in(worker_id)
            kwargs[self.worker_arg_name] = worker_id
            result = fun(**kwargs)
            self.job_exit(worker_id)
            return result

        return inner

    def get_worker(self) -> Hashable:
        """return the worker"""
        worker_id, n_job_running = min(self.job_counter.items(), key=operator.itemgetter(1))
        if n_job_running >= self.max_job_per_worker:
            if self.if_no_id == "wait":
                time.sleep(self.waiting_time)
                return self.get_worker()
            else:
                raise ValueError("No device available now!")
        else:
            return worker_id

    def job_in(self, worker_id: int) -> None:
        self.job_counter[worker_id] += 1

    def job_exit(self, worker_id: int) -> None:
        cnt = self.job_counter[worker_id]
        self.job_counter[worker_id] = max(cnt - 1, 0)


if __name__ == "__main__":
    # example code below:
    import random

    @JobAssigner(["a", "b", "c"], 2, worker_arg_name="worker_id", if_no_id="wait", waiting_time=4)
    def foobar(a, b, worker_id):
        time.sleep(random.randint(12, 15))
        return [a, b, worker_id]

    list_kwargs = [dict(a=i, b=i + 5) for i in range(15)]
    futures = []
    with ThreadPoolExecutor(max_workers=9) as executor:
        for kwargs in list_kwargs:
            future = executor.submit(foobar, **kwargs)
            futures.append(future)

        for idx, future in enumerate(as_completed(futures)):
            print(idx, future.result())
