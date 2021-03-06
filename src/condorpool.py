'''Submit htcondor jobs with an interface like multiprocessing.Pool'''

from __future__ import print_function
import htcondor
import pickle
import os
import uuid
from time import sleep
import datetime
import subprocess
import sys
from socket import gethostname


class TmpCd(object):
    '''Temporarily cd to a directory. Use with:
    with TmpCd('/tmp'):
        <do something in /tmp>
    '''

    def __init__(self, targetdir):
        '''Give the directory to which to cd.'''
        self.target = targetdir
        self.pwd = None
        
    def __enter__(self):
        '''cd to the target dir.'''
        self.pwd = os.getcwd()
        os.chdir(self.target)

    def __exit__(self, *args):
        '''cd back to the original dir.'''
        os.chdir(self.pwd)


class TimeoutError(Exception):
    '''Exception class for timeouts.'''
    def __init__(self, timeout):
        super(TimeoutError, self).__init__('Timeout after {0} s!'
                                           .format(timeout))


class UnsubmittedError(Exception):
    '''Exception class for unsubmitted jobs.'''
    pass


class JobFailedError(Exception):
    '''Exception class for failed jobs.'''
    def __init__(self, job):
        msg = '''Job {0} failed!
Submitted: {4}
Completed: {5}
Output exists: {6}
*** stdout:
{1}

*** stderr:
{2}

*** Log:
{3}
'''.format(job.clusterid, job.stdout(), job.stderr(), job.log(),
           job.submitted(), job.completed(), job.output_exists())
        super(JobFailedError, self).__init__(msg)


class PoolClosedError(Exception):
    '''Exception for closed Pools.'''
    def __init__(self):
        super(PoolClosedError, self).__init__("Can't submit to a closed Pool!")


def add_kerberos_tokens():
    '''Add kerberos tokens to send with jobs.'''
    # From https://batchdocs.web.cern.ch/local/pythonapi.html
    col = htcondor.Collector()
    credd = htcondor.Credd()
    credd.add_user_cred(htcondor.CredTypes.Kerberos, None)
    return col, credd


class RemoteTaskBase(object):
    '''Base class for remote tasks. Creates the job files, retrieves
    the return value, and cleans up, but doesn't implement any 
    mechanisms for submitting/running.'''
    def __init__(self, target, args=(), kwargs={},
                 submitdir='.', cleanup=True, cleanupfiles=[],
                 cleanfailed=False, sendenv=True, cd=False,
                 prefix='remotetask'):
        self.target = target
        self.submitdir = submitdir
        self.args = tuple(args)
        self.kwargs = dict(kwargs)
        self.sendenv = sendenv
        self.cleanup = cleanup
        self.cleanupfiles = list(cleanupfiles)
        self.cleanfailed = cleanfailed
        self.cd = cd
        
        # Get the input and output file names
        self.fname = prefix + '_' + str(uuid.uuid4().hex)
        self.fin = self.fname + '.fin.py'
        self.fout = self.fname + '.fout.pkl'
        self.cleanupfiles += [self.fin, self.fout]

    def __del__(self):
        '''Clean up files.'''
        if not self.cleanup or (not self.cleanfailed
                                and not self.successful()):
            return
        
        with TmpCd(self.submitdir):
            for fname in self.cleanupfiles:
                try:
                    os.remove(fname)
                except OSError:
                    pass
        
    def create_job_file(self):
        '''Create the job file.'''
        # Create the input file
        targetargs = {'target' : self.target, 'args' : self.args,
                      'kwargs' : self.kwargs}
        try:
            pklargs = pickle.dumps(targetargs)
        except pickle.PicklingError as ex:
            raise pickle.PicklingError(
                'Failed to pickle target function or arguments!\n'
                'Got: {0!r}\n'.format(targetargs) + ex.args[0]
            )
        
        with TmpCd(self.submitdir):
            with open(self.fin, 'w') as fin:
                fin.write('''#!/usr/bin/env python{pyver}
from __future__ import print_function
import pickle, os
from pprint import pprint

{cd}

print('Working dir:', os.getcwd())
print('Working dir contents:')
pprint(os.listdir('.'))
print()

{env}
print('Environment:')
pprint(os.environ)

# Unpickle the function and its args
target = pickle.loads({pklargs!r})
print('Got inputs:')
pprint(target)
print()

# Call it.
pwd = os.getcwd()
retval = target['target'](*target['args'], **target['kwargs'])
os.chdir(pwd)

print('Got return value:')
pprint(retval)
print()

print('Working dir contents after exe:')
pprint(os.listdir('.'))
print()

# Pickle the return value first so that the output
# file isn't created in case of pickling errors.
try:
    retpkl = pickle.dumps(retval)
except pickle.PicklingError as ex:
    raise pickle.PicklingError(
        'Failed to pickle return value: ' + repr(retval)
        + '\\n' + ex.args[0]
    )

# Write the pickled return value it to the output file.
with open({fout!r}, 'wb') as fout:
    fout.write(retpkl)

print('Working dir contents after pickle output:')
pprint(os.listdir('.'))
    '''.format(pklargs = pklargs, fout = self.fout,
               pyver=sys.version_info.major,
               cd=('' if not self.cd else
                   'os.chdir({!r})'.format(self.submitdir)),
               env=('' if not self.sendenv else
                    'os.environ.update({!r})'.format(os.environ))))
            os.chmod(self.fin, 0o700)
        return os.path.join(self.submitdir, self.fin)
    
    def output_exists(self):
        '''Check if the outputfile exists.'''
        with TmpCd(self.submitdir):
            return os.path.exists(self.fout)

    def successful(self):
        '''Check if the job was successful.'''
        return self.output_exists()
        
    def _return_value(self):
        '''Get the return value of the job.'''
        with TmpCd(self.submitdir):
            with open(self.fout, 'rb') as fout:
                retval = pickle.load(fout)
        return retval

    def _failed(self):
        '''Called when the job fails.'''
        raise Exception('Task failed!')        
    
    def get(self):
        '''Get the return value of the job.'''
        if self.successful():
            return self._return_value()
        self._failed()
        

class SshTask(RemoteTaskBase):
    '''Run a job over ssh.'''
    def __init__(self, remote, *args, **kwargs):
        '''remote: address of the server on which to execute
        Other args are the same as RemoteTaskBase. Assumes
        the job is run from a shared directory accessible to
        the remote server.'''
        # Protect against recursive sshing
        if gethostname().startswith(remote):
            raise Exception('Attempt to ssh to host!')
        self.remote = remote
        kwargs['cd'] = True
        kwargs['submitdir'] = os.path.abspath(kwargs.get('submitdir', '.'))
        super(SshTask, self).__init__(*args, **kwargs)
        self._stdout = self.fname + '.stdout'
        self._stderr = self.fname + '.stderr'
        self.cleanupfiles += [self._stdout, self._stderr]
        self.proc = None
        
    def submit(self):
        '''Submit the job.'''
        jobfile = self.create_job_file()

        args = ['ssh', self.remote]
        if not self.sendenv:
            args += ['python', jobfile]
        else:
            env = '\n'.join(['export {0}={1!r}'.format(var, val)
                             for var, val in os.environ.items()
                             if var != '_'])
            args += ['bash', '-c', '''
{env}
python {fin}
'''.format(env=env, fin=os.path.join(self.submitdir, self.fin))]
        with TmpCd(self.submitdir):
            stdout = open(self._stdout, 'w')
            stderr = open(self._stderr, 'w')
            self.proc = subprocess.Popen(
                args, stdout=stdout, stderr=stderr
            )
        return self.proc.pid

    def wait(self):
        '''Wait for the job to complete.'''
        if not self.proc:
            return
        return self.proc.wait()

    def get(self):
        '''Get the return value.'''
        self.wait()
        return super(SshTask, self).get()
    
    def stdout(self):
        '''Get the stdout.'''
        stdout = os.path.join(self.submitdir,
                              self._stdout)
        if not os.path.exists(stdout):
            return ''
        with open(stdout) as f:
            return f.read()

    def stderr(self):
        '''Get the stderr.'''
        stderr = os.path.join(self.submitdir,
                              self._stderr)
        if not os.path.exists(stderr):
            return ''
        with open(stderr) as f:
            return f.read()

    def successful(self):
        '''Check if the job was successful.'''
        return (super(SshTask, self).successful()
                and self.proc.poll() == 0)
    
    def _failed(self):
        '''Called when the job fails.'''
        raise Exception(
            '''Task failed!
exit code: {2}
stdout:
{0}

stderr:
{1}
'''.format(self.stdout(), self.stderr(), self.proc.poll() if self.proc else None)
        )
    
        
class Job(RemoteTaskBase):
    '''Submit a job to condor which executes a python function with the 
    given arguments. The interface mimics that of 
    multiprocessing.pool.ApplyResult in that it has get, ready, successful,
    and wait functions that behave as expected.'''

    statuscodes = {1 : 'Idle',
                   2 : 'Running',
                   3 : 'Removing',
                   4 : 'Completed',
                   5 : 'Held',
                   6 : 'Transferring Output',
                   7 : 'Suspended'}

    @staticmethod
    def status_from_code(code):
        '''Get the job status from the status code.'''
        return Job.statuscodes[int(code)]
    
    def __init__(self, target, args = (), kwargs = {}, submitkwargs = {},
                 submitdir = '.', cleanup = True, cleanupfiles = [],
                 polltime = 60, killstats = ('Held', 'Suspended'),
                 cleanfailed=False):
        '''Makes a condor job. target, args, kwargs and the return value of
        target must all be picklable. By default, the current environment is
        sent with the job (getenv = True) unless 'environment' is given in
        submitkwargs.
        - target: the python function to be called.
        - args & kwargs: the arguments to be passed to target
        - submitdir: directory from which to submit the job and copy the
          output files (stdout, stderr, log, and return value)
        - submitkwargs: the dict to be passed the htcondor.Submit instance, see
        https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html#submit-description-file-commands
          If running at CERN, include 'MY.SendCredential' = True in the dict to send kerberos
          tokens with the job.
        - cleanup: delete temporary files created by the job when it's
          deleted. Default to the input script, stdout, stderr and log.
        - cleanupfiles: extra files to delete when the job is deleted.
        - polltime: default poll interval for wait.
        - killstats: if the Job status is any of these when it's deleted the
          job will be killed.
        - cleanfailed: whether to clean up job files for failed jobs
        '''
        super(Job, self).__init__(
            prefix='condorpool', target=target, args=args,
            kwargs=kwargs, submitdir=submitdir, cleanup=cleanup,
            cleanupfiles=cleanupfiles, cleanfailed=cleanfailed,
            sendenv=True)
                                  
        self.submitkwargs = dict(submitkwargs)
        self.polltime = polltime
        self.killstats = killstats
        
        # Take the current environment if not given
        if 'environment' not in submitkwargs:
            self.submitkwargs['getenv'] = 'True'

        # Set them as the arguments to the main function.
        self.submitkwargs['executable'] = self.fin
        
        # Transfer the input file to the worker
        if 'transfer_input_files' in self.submitkwargs:
            self.submitkwargs['transfer_input_files'] += ',' + self.fin
        else:
            self.submitkwargs['transfer_input_files'] = self.fin
        # If output files are specified, add the output file to the list
        # of files to transfer back. Otherwise, all files should be
        # transfered back.
        if 'transfer_output_files' in self.submitkwargs:
            self.submitkwargs['transfer_output_files'] += ',' + self.fout

        for name in 'output', 'error', 'log':
            self.submitkwargs[name] = self.fname + '.' + name[:3]
            self.cleanupfiles.append(self.submitkwargs[name])

        self.clusterid = None
        
    def __del__(self):
        '''Wait for the job to complete and delete temporary files created by
        the job.'''
        if not self.cleanup:
            return
        # Input file is only created on submit.
        if not self.submitted():
            return

        # Wait for the job to make sure all output files have been
        # returned. Kill it if it's Suspended or Held.
        try:
            self.wait(killstats = self.killstats)
        except JobFailedError as ex:            
            print('JobFailedError:', ex.args[0], file = sys.stderr)
            if not self.cleanfailed:
                return
            
        super(Job, self).__del__()
        
    def submit(self, maxtries = 5, wait = 20):
        '''Submit the job.
        - maxtries: number of attemps to submit (in case of timeouts)
        - wait: wait time between submission attempts'''
        # Create the input file
        self.create_job_file()

        # Weirdly, inheriting from htcondir.Submit doesn't work as
        # super(Job, self).__init__ returns Job.__init__, causing
        # an infinite recursion. So just make a Submit instance here.
        _submit = htcondor.Submit(**self.submitkwargs)

        with TmpCd(self.submitdir):
            # Add the job to the queue
            schedd = htcondor.Schedd()
            with schedd.transaction() as txn:
                # In case of timeouts, try five times
                nfail = 0
                try:
                    self.clusterid = _submit.queue(txn)
                except htcondor.HTCondorIOError as ex:
                    nfail += 1
                    if nfail == maxtries:
                        raise htcondor.HTCondorIOError(
                            ex.args[0] + '\nFailed {0} times.'.format(maxtries)
                        )
                    sleep(wait)
        return self.clusterid

    def constraint(self):
        '''Get the constraint to select this job in the job scheduler.'''
        return 'ClusterID == ' + str(self.clusterid)
        
    def query(self, *projection, **kwargs):
        '''Query the scheduler about this job. Default the job status. 
        See
        https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.html
        for the list of attributes that can be given in 'projection'.
        If the job is completed, this checks the history unless 
        checkhistory = False is given. Checking the history can
        be slow.'''
        if not self.submitted():
            return
        if not projection:
            projection = ['JobStatus']
        projection = list(projection)
        schedd = htcondor.Schedd()
        query = schedd.query(constraint = self.constraint(),
                             projection = projection)
        # Job is still running.
        if query or not kwargs.get('checkhistory', True):
            return query
        # Job is completed and is in the history.
        # This is quite slow.
        return list(schedd.history(self.constraint(), projection))

    def act(self, action):
        '''Perform a scheduling action on this job.'''
        schedd = htcondor.Schedd()
        if isinstance(action, int):
            action = Job.actions[action]
        schedd.act(action, self.constraint())        
    
    def analyze(self):
        '''Run condor_q -analyze <id>.'''
        proc = subprocess.Popen(['condor_q', '-analyze', str(self.clusterid)],
                                stdout = subprocess.PIPE,
                                stderr = subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if sys.stdout.encoding is not None:
            stdout = str(stdout.decode(sys.stdout.encoding))
            stderr = str(stderr.decode(sys.stdout.encoding))
        return stdout
        
    def status(self):
        '''Check the status of the job.'''
        if not self.submitted():
            return
        # Avoid checking the history as it's slow. If it's not in the queue
        # then it's completed.
        stat = self.query('JobStatus', checkhistory = False)
        if not stat:
            return 'Completed'
        return Job.status_from_code(stat[0].get('JobStatus'))
        
    def submitted(self):
        '''Check if the job has been submitted.'''
        return self.clusterid is not None

    def completed(self):
        '''Check if the job has completed.'''
        return self.status() == 'Completed'

    def ready(self):
        '''Check if the job has completed.'''
        return self.completed()
    
    def successful(self):
        '''Check if the job completed successfully. Returns None if the job
        isn't completed.'''
        # The output file will only exist if the job completed
        # successfully.
        if self.output_exists():
            return True
        # Not submitted
        if not self.submitted():
            return
        # Not completed
        if not self.completed():
            return
        # Failed
        return False
    
    def wait(self, timeout = None, polltime = None, timeouterror = False,
             killstats = None):
        '''Wait til the job is completed. Check every 'polltime'
        seconds (default given in the constructor). If timeout is
        given, wait timeout seconds before giving up. If 
        timeouterror = True, raise a TimeoutError after a timeout.
        If killstats is given, kill the job if its status is any
        of those in killstats and raise a JobFailedError.'''
        if not self.submitted():
            raise UnsubmittedError("Can't wait on a job that's not"
                                   " been submitted!")
        # Already done
        if self.successful():
            return
        
        if timeout:
            start = datetime.datetime.today()

            def checktimeout():
                delta = (datetime.datetime.today() - start).total_seconds()
                return delta < timeout
        else:
            def checktimeout():
                return True

        if polltime is None:
            polltime = self.polltime
        if killstats is None:
            killstats = self.killstats
        while checktimeout():
            nfailed = 0
            try:
                status = self.status()
                nfailed = 0
            # Catch timeouts of the Condor scheduler query
            except htcondor.HTCondorIOError as ex:
                status = ''
                nfailed += 1
                if nfailed == 5:
                    htcondor.HTCondorIOError(
                        ex.args[0]
                        + '\ncondorpool.Job.wait: Quit after 5 '
                        + 'consecutive failed status queries.'
                    )
            if status == 'Completed':
                return
            elif status in killstats:
                analysis = self.analyze()
                self.remove()
                ex = JobFailedError(self)
                msg = ex.args[0].splitlines()
                msg = '\n'.join([msg[0], 'Status ' + repr(status)
                                 + ' caused it to be killed']
                                + msg[1:] + ['*** Analysis:', analysis])
                ex.args = (msg,)
                raise ex
            sleep(polltime)

        if timeouterror:
            raise TimeoutError(timeout)

    def wait_to_start(self, timeout = None, polltime = None):
        '''Wait for the job to start.'''
        if timeout:
            start = datetime.datetime.today()

            def checktimeout():
                delta = (datetime.datetime.today() - start).total_seconds()
                return delta < timeout
        else:
            def checktimeout():
                return True

        if self.status() != 'Idle':
            return

        if polltime is None:
            polltime = self.polltime
        while checktimeout():
            sleep(self.polltime)
            if self.status() != 'Idle':
                return
        
    def get_text_output(self, ftype = 'output'):
        '''Get the contents of a text output file. ftype can be
        'output' (default), 'error', or 'log'. Returns None if
        the file doesn't exist.'''
        if ftype not in ('output', 'error', 'log'):
            raise ValueError('Unknown ftype: ' + str(ftype))
        fname = self.submitkwargs[ftype]
        with TmpCd(self.submitdir):
            if not os.path.exists(fname):
                return
            with open(fname) as f:
                return f.read()
        
    def stdout(self):
        '''Get the stdout of the job.'''
        return self.get_text_output()

    def stderr(self):
        '''Get the stderr of the job.'''
        return self.get_text_output('error')

    def log(self):
        '''Get the log of the job.'''
        return self.get_text_output('log')

    def get(self, timeout = None, polltime = None, killstats = ()):
        '''Get the result of the job. If successful, returns the
        return value of the function, otherwise raises an exception
        and outputs all available info.'''

        self.wait(timeout, polltime, timeouterror = True,
                  killstats = killstats)
        return super(Job, self).get()

    def _failed(self):
        '''Called when the job fails.'''
        raise JobFailedError(self)


Job.actions = {}
for action in ('Continue', 'Hold', 'Release', 'Remove',
               'RemoveX', 'Suspend', 'Vacate', 'VacateFast'):
    setattr(Job, (action.lower() if action != 'Continue' else action),
            eval('lambda self : self.act(htcondor.JobAction.{0})'
                 .format(action)))
    action = getattr(htcondor.JobAction, action)
    Job.actions[action.real] = action


def _remote_run(self, target, *args, **kwargs):
    '''Pickle-able function to call a member method.'''
    return getattr(self, target)(*args, **kwargs)
    
    
class RemoteJob(Job):
    '''Submit a job over ssh.'''
    def __init__(self, remote, *args, **kwargs):
        '''remote: the ssh server to submit from.
        The other args are passed to Job.'''
        self.remote = remote
        super(RemoteJob, self).__init__(*args, **kwargs)

    def _remote_run(self, target, *args, **kwargs):
        '''Run on the remote server.'''
        task = SshTask(self.remote, _remote_run,
                       # Make the Job instance from the RemoteJob
                       args=(self.local_job(), target) + args,
                       kwargs=kwargs,
                       submitdir=self.submitdir)
        task.submit()
        return task.get()
        
    def submit(self, maxtries=5, wait=20):
        '''Submit the job.
        - maxtries: number of attemps to submit (in case of timeouts)
        - wait: wait time between submission attempts'''
        self.clusterid = self._remote_run('submit', maxtries=maxtries,
                                          wait=wait)

    def query(self, *projection, **kwargs):
        '''Query the scheduler about this job. Default the job status. 
        See
        https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.html
        for the list of attributes that can be given in 'projection'.
        If the job is completed, this checks the history unless 
        checkhistory = False is given. Checking the history can
        be slow.'''
        return self._remote_run('query', *projection, **kwargs)

    def act(self, action):
        '''Perform a scheduling action on this job.'''
        # Convert to int with action.real as htcondor.JobAction
        # instances can't be pickled
        return self._remote_run('act', action.real)

    def analyze(self):
        '''Perform a scheduling action on this job.'''
        return self._remote_run('analyze')

    def wait(self, *args, **kwargs):
        '''Wait til the job is completed. Check every 'polltime'
        seconds (default given in the constructor). If timeout is
        given, wait timeout seconds before giving up. If 
        timeouterror = True, raise a TimeoutError after a timeout.
        If killstats is given, kill the job if its status is any
        of those in killstats and raise a JobFailedError.'''
        # Already completed
        if self.successful():
            return
        return self._remote_run('wait', *args, **kwargs)

    def local_job(self):
        '''Make a (local) Job instance from this RemoteJob and
        set cleanup=False so it doesn't wait or delete files when
        deleted.'''
        j = Job(self.target)
        # Copy attributes
        for k, v in self.__dict__.items():
            setattr(j, k, v)
        j.cleanup = False
        return j


class MapResult(object):
    '''Results of a mapping of a sequence onto a set of jobs.'''
    def __init__(self, jobs):
        '''Takes the list of jobs.'''
        self.jobs = jobs

    def get(self, timeout = None, polltime = 5):
        '''Get the results.'''
        self.wait(timeout, polltime, timeouterror = True)
        return [j.get() for j in self.jobs]

    def ready(self):
        '''Check if all jobs are ready.'''
        return all(j.ready() for j in self.jobs)

    def successful(self):
        '''Check if all jobs are successful.'''
        return all(j.successful() for j in self.jobs)

    def wait(self, timeout = None, polltime = 5, timeouterror = False,
             killstats = ()):
        '''Wait for all jobs to finish.'''
        if not all(j.submitted() for j in self.jobs):
            raise UnsubmittedError("Can't wait on a job that's not been"
                                   "submitted!")

        if timeout is not None:
            for j in self.jobs:
                start = datetime.datetime.today()
                j.wait(timeout, polltime, timeouterror, killstats)
                end = datetime.datetime.today()
                timeout -= (end - start).total_seconds()
        else:
            for j in self.jobs:
                j.wait()


class Pool(object):
    '''Interface to condor that mimics multiprocessing.Pool.'''

    def __init__(self, submitkwargs = {}, jobkwargs = {},
                 killstats = ('Held', 'Suspended'), remote=None):
        '''- submitkwargs: a default set of kwargs for htcondor.Submit instances.
          See:
          https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html#submit-description-file-commands
          These can be updated per job using the 'submitkwargs' argument to 
          apply_async, etc.
          If running at CERN, include 'MY.SendCredential' = True in the dict to send kerberos
          tokens with the job.
        - jobkwargs: a default set of kwargs for Job instances (excluding 
          submitkwargs). See help(Job).
        - killstats: Jobs with any of these statuses will be killed when the
          Pool is closed.'''
        self.submitkwargs = dict(submitkwargs)
        self.jobkwargs = dict(jobkwargs)
        self.killstats = killstats
        self.jobs = []
        self.make_job = Job
        self.remote = remote
        if self.remote:

            def make_job(*args, **kwargs):
                return RemoteJob(self.remote, *args, **kwargs)

            self.make_job = make_job
        
    def __del__(self):
        '''Close the Pool.'''
        # May already be closed if 'with' was used, but in case
        # not, close on delete. There's no harm in closing twice.
        try:
            self.close()
        except JobFailedError as ex:
            print('JobFailedError:', ex.args[0], file = sys.stderr)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        '''Close the Pool.'''
        self.close()
        
    def clear_completed(self):
        '''Remove completed jobs from the pool's list.'''
        self.jobs = list(filter(lambda j : not j.completed(), self.jobs))

    def clear_successful(self):
        '''Remove successful jobs from the pool's list.'''
        self.jobs = list(filter(lambda j : not j.successful(), self.jobs))

    def terminate(self):
        '''Terminate all active jobs.'''
        for j in self.jobs:
            if not j.completed():
                j.remove()

    def close(self):
        '''Close this pool: wait for all current jobs to complete (unless they
        were submitted with cleanup = False) and accept no more jobs. Jobs
        that are currently Held or Suspended will be killed (again, unless
        submitted with cleanup = False).'''
        if self.closed():
            return
        self.apply_async = self._closed_apply_async
        exceptions = []
        # Collect the exceptions for all failed jobs
        while self.jobs:
            try:
                j = self.jobs.pop()
                # Wait for the job to complete. Kill it if it's Suspended or
                # Held.
                j.get(killstats = self.killstats)
            except JobFailedError as ex:
                exceptions.append(ex)
        if exceptions:
            msg = 'Pool.close - failed jobs:\n'
            for ex in exceptions:
                msg += '=' * 10 + '\n' + ex.args[0]
            ex.args = (msg,)
            raise ex

    def closed(self):
        '''Check if the Pool is closed.'''
        return self.apply_async == self._closed_apply_async
        
    def _closed_apply_async(self, *args, **kwargs):
        '''apply_async redirects to this after the pool is closed.
        It just raises an exception when called.'''
        raise PoolClosedError()
        
    def apply_async(self, func, args = (), kwds = {},
                    submitkwargs = {}, jobkwargs = {},
                    maxtries=5):
        '''Run a job asynchronously.'''
        _submitkwargs = dict(self.submitkwargs)
        _submitkwargs.update(submitkwargs)
        submitkwargs = _submitkwargs
        _jobkwargs = dict(self.jobkwargs)
        _jobkwargs.update(jobkwargs)
        jobkwargs = _jobkwargs
        if 'killstats' not in jobkwargs:
            jobkwargs['killstats'] = self.killstats
        # Add kerberos tokens if requested by the MY.SendCredential option
        if submitkwargs.get('MY.SendCredential', False):
            # Not sure if we need these to stay in scope during the submission?
            col, credd = add_kerberos_tokens()
        j = self.make_job(func, args = args, kwargs = kwds,
                          submitkwargs = submitkwargs,
                          **jobkwargs)
        j.submit(maxtries=maxtries)
        self.jobs.append(j)
        return j

    def apply(self, *args, **kwargs):
        '''Run a job synchronously and return the function's return value.
        Takes the same arguments as apply_async.'''
        j = self.apply_async(*args, **kwargs)
        return j.get()

    def map_async(self, func, iterable, **kwargs):
        '''Map arguments in 'iterable' with 'func' and run
        asynchronously. kwargs is passed to apply_async.'''
        jobs = []
        for i in iterable:
            j = self.apply_async(func, args = (i,),
                                 **kwargs)
            jobs.append(j)
        return MapResult(jobs)

    def map(self, func, iterable, **kwargs):
        '''Map arguments in 'iterable' with 'func' and run
        synchronously. kwargs is passed to apply_async.'''
        result = self.map_async(func, iterable, **kwargs)
        return result.get()

    def imap(self, func, iterable, **kwargs):
        '''Like map but returns an iterator. kwargs is passed to
        apply_async.'''
        # Not really what imap should do, but it works anyway
        return iter(self.map(func, iterable, **kwargs))

    imap_unordered = imap

    
def test(a, b, c):
    '''Simple test function.'''
    return a, b * c


def unpickleable():
    '''Return something unpickleable - a lambda function.'''
    return lambda x : x
