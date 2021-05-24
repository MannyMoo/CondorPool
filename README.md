# CondorPool

This python package provides a worker-pool type interface like that of [`multiprocessing.Pool`](`https://docs.python.org/3/library/multiprocessing.html`) to an HTCondor batch system. It builds on the [`htcondor`](https://pypi.org/project/htcondor/) python API, which you can install with

``` bash
pip install htcondor
```

(or whichever variant of `pip` works for you). Then do

``` bash
git clone https://github.com/MannyMoo/CondorPool.git
```

and add `CondorPool/src` to `PYTHONPATH` by whichever method works for you, eg for bash shells:

``` bash
export PYTHONPATH=/<path>/<to>/CondorPool/src:$PYTHONPATH
```

The interface is identical to that of `multiprocessing.Pool`. Running on a machine that has access to an HTCondor batch system, you can do, eg:

``` python
from condorpool import Pool
from math import sqrt

with Pool() as pool:
    # Call a function and wait to get its result
    result = pool.apply(sqrt, args = (2,))
    
    # Call a function asynchronously and retrieve its result at
    # a later time
    job = pool.apply_async(sqrt, args = (2,))
    # Get the result, waiting for the job to complete if necessary
    result = job.get()
    
    # Map a sequence of arguments with a function
    results = pool.map(sqrt, range(5))
    
    # Do the same asynchronously
    mapjob = pool.map_async(sqrt, range(5))
    # Get the results, waiting for all jobs
    results = mapjob.get()
```

For more info, see 

``` python
from condorpool import Job, Pool

help(Pool)
help(Job)
```

As with `multiprocessing.Pool`, the function to call, its arguments, and its return value must all be [picklable](https://docs.python.org/3/library/pickle.html) in order to send them to and from the batch system.

Tested with python 3.8.10 & `htcondor` 9.0.0, python 2.7.16 & `htcondor` 9.0.0, and Condor 9.0.1.
