from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Union

from tqdm import tqdm


def run_jobs_with_threading(
    max_workers: int,
    list_kwargs: List[Dict[str, Any]],
    fun: Callable,
    timeout: Union[float, None] = None,
) -> Dict[int, Dict]:
    future_arg_dic = dict()
    result_dic = dict()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with tqdm(desc="threading_jobs", total=len(list_kwargs)) as pbar:
            for kwargs in list_kwargs:
                future = executor.submit(fun, **kwargs)
                future_arg_dic[future] = kwargs

            for idx, future in enumerate(as_completed(future_arg_dic, timeout=timeout)):
                kwargs = future_arg_dic[future]
                result = future.result()
                result_dic[idx] = dict(kwarg=kwargs, result=result)
                pbar.update()
    return result_dic
