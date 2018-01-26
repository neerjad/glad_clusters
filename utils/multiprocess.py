import itertools
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool


MAX_PROCESSES=8


def map_with_pool(data_load_func,jobs_list,max_processes=MAX_PROCESSES):
  pool=Pool(processes=min(len(jobs_list),max_processes))
  dfs=_run_procs(pool,data_load_func,jobs_list)
  _stop_pool(pool)
  return dfs.get()


def map_with_threadpool(data_load_func,jobs_list,max_processes=MAX_PROCESSES):
  pool=ThreadPool(processes=min(len(jobs_list),max_processes))
  dfs=_run_procs(pool,data_load_func,jobs_list)
  _stop_pool(pool)
  return dfs.get()


def _stop_pool(pool):
  pool.close()
  pool.join()
  return True


def _run_procs(pool,map_func,objects):
  try:
    return pool.map_async(map_func,objects)
  except KeyboardInterrupt:
    print("Caught KeyboardInterrupt, terminating workers")
    pool.terminate()
    return False
  else:
    print("Normal termination")
    pool.close()
    pool.join()
    return False
