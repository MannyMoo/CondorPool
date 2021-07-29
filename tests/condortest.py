#!/usr/bin/env python

'''Test condorpool.Pool.'''

from __future__ import print_function
from condorpool import Pool, test, JobFailedError, TimeoutError,\
    PoolClosedError, unpickleable
from math import sqrt
from time import sleep
import pickle

if __name__ == '__main__':
    # Testing code.
    try:
        with Pool(jobkwargs = {'submitdir' : '/tmp', 'polltime' : 5},
                  submitkwargs = {'requirements' : 'OpSysAndVer == "CentOS7"'}) as pool:
            print('*** Basic test')
            assert test(1, 2, 3) == pool.apply(test, (1,), {'b' : 2, 'c' : 3})
            print()

            print('*** Test call failure')
            try:
                pool.apply(sqrt, ('a',))
            except JobFailedError as ex:
                print('Caught JobFailedError:', ex.args[0])
            print()

            print('*** Test pickle input failure')
            try:
                pool.apply(lambda x : x, (1,))
            except pickle.PicklingError as ex:
                print('Caught PicklingError:', ex.args[0])
            print()

            print('*** Test pickle output failure')
            try:
                pool.apply(unpickleable)
            except JobFailedError as ex:
                print('Caught JobFailedError:', ex.args[0])
            print()

            print('*** Test kill')
            j = pool.apply_async(sleep, (10,))
            sleep(3)
            j.remove()
            print(j.log())
            print()

            print('*** Test timeout')
            j = pool.apply_async(sleep, (10,))
            try:
                j.get(1, 1)
            except TimeoutError as ex:
                print('Caught TimeoutError:', ex.args[0])
                j.get()
            print()

            print('*** Test map')
            assert list(map(sqrt, range(3))) == pool.map(sqrt, range(3))
            print()

            print('*** Test map timeout')
            result = pool.map_async(sleep, range(5, 8))
            try:
                result.get(1, 1)
            except TimeoutError as ex:
                print('Caught TimeoutError:', ex.args[0])
                result.get()
            print()

            print('*** Test terminate')
            pool.clear_completed()
            result = pool.map_async(sleep, range(30, 33))
            sleep(3)
            pool.terminate()
            print('Jobs:', pool.jobs)
            pool.clear_successful()
            print('Unsuccessful jobs:', pool.jobs)
            assert len(pool.jobs) == 3
            pool.clear_completed()
            assert len(pool.jobs) == 0
            print()

            # Test a job that gets held - require a non-existent input file
            j = pool.apply_async(sqrt, (3,),
                                 submitkwargs = {'transfer_input_files' : 'spam'})
            # Wait for it to be held
            j.wait_to_start()
            # Test suspending a job.
            j = pool.apply_async(sleep, (10,))
            # Wait til it's running.
            j.wait_to_start()
            j.suspend()
            del j
            # Wait to make sure its status has changed
            sleep(3)
            
            # Submit a job that will fail
            pool.apply_async(sqrt, ('a',))

            # Submit some active jobs
            pool.map_async(sleep, range(10))

            print('*** Test close with active/suspended/held/failed jobs')

    except JobFailedError as ex:
        print('Caught JobFailedError:', ex.args[0])
        
    print('*** Test submit to closed pool')
    try:
        pool.apply(sqrt, (3,))
    except PoolClosedError as ex:
        print('Caught PoolClosedError:', ex.args[0])

