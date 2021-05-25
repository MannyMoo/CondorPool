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

The interface is identical to that of `multiprocessing.Pool`. As with `multiprocessing.Pool`, the function to call, its arguments, and its return value must all be [picklable](https://docs.python.org/3/library/pickle.html) in order to send them to and from the batch system.

Running on a machine that has access to an HTCondor batch system, you can do, eg:

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

When the `Pool` exits from a `with` statement, goes out of scope, or has `close()` called manually, it will wait til all active `Job`s are completed. This ensures that the `Job`s complete successfully (an exception will be raised if not) and they clean up files created in the submission directory (main script, stdout, stderr, log, and return value `.pkl`). By default, if a `Job` is Held or Suspended when the `Pool` closed, it's killed. You can manage which status types are killed on close with the constructor argument `killstats`.

If you don't want the `Pool` to wait (and don't care about the return value), you can create a `Pool` with:

``` python
Pool(jobkwargs = {'cleanup' : False, 'submitdir' : '/tmp'})
```

This way, the temporary files created by the `Job`s will be written to `/tmp` (which you may want to do in any case) and the `Job`s will go out of scope without waiting to complete or cleaning up after themselves. `jobkwargs` can also be given per-job by passing it as an argument to any `Pool` function that creates a job (`apply`, `map`, etc).

You can configure the parameters of the job with the `submitkwargs` constructor argument to `Pool` or by passing `submitkwargs` to any `Pool` function that creates a `Job`. Eg, to require a CentOS7 machine, use

``` python
submitkwargs = {'requirements' : 'OpSysAndVer == "CentOS7"'}
```

By default, the submission environment is sent with the jobs (`getenv = True`) unless `"environment"` is set in `submitkwargs`. See [here](https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html#submit-description-file-commands) for the full list of Condor job parameters.

For more info, see 

``` python
from condorpool import Job, Pool

help(Pool)
help(Job)
```

Tested with python 3.8.10 & `htcondor` 9.0.0, python 2.7.16 & `htcondor` 9.0.0, and Condor 9.0.1.
