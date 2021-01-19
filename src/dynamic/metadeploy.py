# This file contains function handles to control spawning and destroying entire clusters,
# to deploy applications and data on running clusters,
# and to clean up after application execution.

from enum import Enum
import os
import sys
import time

from config.meta import cfg_meta_instance as metacfg
from remote.reservation import Reserver
from remote.util.deploymode import DeployMode
from util.executor import Executor
import util.fs as fs
import util.location as loc
from util.printer import *

class MetaDeployState(Enum):
    '''Possible deployment states'''
    INIT = 0     # Not working yet
    BUSY = 1     # Working
    COMPLETE = 2 # Finished, with success
    FAILED = 3   # Finished, with failure


class MetaDeploy(object):
    '''Object to dynamically pass to meta deployment setups'''
    
    def __init__(self):
        # We store the deploymode last used to start clusters, 
        # so we can clean the junk dir in the right location
        self._deploymode = None 
    '''
    Block for given command
    Command must return a MetaDeployState, optionally with an additional value
    If the 'COMPLETE' state is returned, blocking stops and we return True
    If the 'FAILED' state is returned, blocking stops and we return False
    If the 'BUSY' state is returned, we sleep for sleeptime seconds
    
    Note: If the command returns both a MetaDeployState and an additional value,
    we check the difference with the previous value.
    If the value remains unchanged after dead_after_retries retries, we assume
    that the application has died, and we return False
    '''
    def block(self, command, args=None, sleeptime=60, dead_after_retries=3):
        val = None
        state = MetaDeployState.INIT
        unchanged = 0

        while True:
            if args == None or len(args) == 0:
                tmp = command()
            else:
                tmp = command(*args)
            if len(tmp) == 2:
                state, val_cur = tmp
                if val_cur == val:
                    unchanged += 1
                else:
                    unchanged = 0
                val = val_cur
            else:
                state = tmp

            if state == MetaDeployState.COMPLETE:
                return True # Completed!
            elif state == MetaDeployState.FAILED:
                return False # User function tells we failed

            if unchanged == dead_after_retries:
                printe('Value ({}) did not change in {} times {} seconds!'.format(val, dead_after_retries, sleeptime))
                return False
            time.sleep(sleeptime)


    '''
    Starts a cluster with given time_to_reserve, config_filename, etc.
    If debug_mode is True, we print extra information. Do not use for production.
    deploy_mode determines where we place Spark worker work directories (e.g. on NFS mount, local disks, RAMdisk, local-ssd)
    If no_interact is True, we never ask stuff to the user, useful for running batch-jobs.
    If launch_spark is True, we launch Spark on allocated nodes. Otherwise, we only allocate nodes
    We try to boot the cluster for retries retries. If we fail, we first sleep retry_sleep_time before retrying.
    '''
    def cluster_start(self, time_to_reserve, config_filename, debug_mode, deploy_mode, no_interact, launch_spark=True, retries=5, retry_sleep_time=5):
        self._deploymode = DeployMode.interpret(deploy_mode) if isinstance(deploy_mode, str) else deploy_mode
        for x in range(retries):
            if launch_spark:
                from main import start
                if start(time_to_reserve, config_filename, debug_mode, str(self._deploymode), no_interact):
                    self._deployment = Reserver.load().deployment
                    return True
            else:
                from main import _start_cluster
                self._deployment = _start_cluster(time_to_reserve, config_filename, str(self._deploymode), no_interact).deployment
                return True

            time.sleep(retry_sleep_time)
        return False


    @property
    def deployment(self):
        if self._deployment:
            return self._deployment
        raise RuntimeError('Have no available deployment!')

    '''
    We stop a cluster using this function.
    We try to stop the cluster for retries retries. If we fail, we first sleep retry_sleep_time before retrying.
    '''
    def cluster_stop(self, silent=False, retries=5, retry_sleep_time=5):
        from main import stop
        for x in range(retries):
            if stop(silent):
                return True
            time.sleep(retry_sleep_time)
        return False


    # Remove junk generated during each run. Please use this between runs, not in a run
    def clean_junk(self, fast=False, datadir=None):
        if self._deploymode == None: # We don't know where to clean. Clean everywhere
            workdirs = ' '.join([loc.get_spark_work_dir(val) for val in DeployMode])
            fast = False # we don't know the node numbers!
        else:
            workdirs = loc.get_spark_work_dir(self._deploymode)

        if fast:
            nfs_log = loc.get_spark_logs_dir()
            command = 'rm -rf {} {}'.format(workdirs, nfs_log)
            return os.system(command) == 0
        else:
            executors = []
            nfs_log = loc.get_spark_logs_dir()
            log_command = 'rm -rf {}'.format(nfs_log)
            executors.append(Executor(log_command, shell=True))
            datadir = '' if datadir == None else datadir

            for x in self._deployment.nodes:
                clean_command = 'ssh {} "rm -rf {} {} {}"'.format(x, workdirs, nfs_log, datadir)

                executors.append(Executor(clean_command, shell=True))
            Executor.run_all(executors)
            state = Executor.wait_all(executors, stop_on_error=False)
            if state:
                prints('Clean success!')
            else:
                printe('Clean failure!')
            return state

    '''
    Deploy an application. We require the following parameters:
    jarfile name (which exists in <project root>/jars/),
    mainclass inside the jarfile,
    args for the jar (can be None),
    extra_jars (which exist in <project root>/jars/) to submit alongside the jarfile (can be None),
    submit_opts (str) extra options for spark-submit (for advanced users),
    no_resultdir (bool) to indicate whether we should skip making a result directory or not,
    flamegraph (str) to indicate whether we want to record data for a flamegraph (looks like 30s, 2m, 4h, can be None)
    retries for trying to deploy the application. If we fail, we first sleep retry_sleep_time before retrying.
    '''
    def deploy_application(self, jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir, flamegraph=None, retries=5, retry_sleep_time=5):
        from deploy.deploy import _deploy_application_internal
        for x in range(retries):
            if _deploy_application_internal(jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir, flamegraph!=None, flamegraph):
                return True
            time.sleep(retry_sleep_time)
        return False

    '''
    Deploy an application (which is not a Spark application) on all nodes to run in parallel.
    Useful to e.g. generate data
    '''
    def deploy_nonspark_application(self, command):
        try:
            reserver = Reserver.load()
        except FileNotFoundError as e:
            printe('No reservation found on remote. Cannot run!')
            return False
        except Exception as e:
            printe('Reservation file found, no longer active')
            return False
        executors = []
        for host in reserver.deployment.nodes:
            executors.append(Executor('ssh {} "{}"'.format(host, command), shell=True))
        Executor.run_all(executors)
        state = Executor.wait_all(executors, stop_on_error=False)
        if state:
            prints('Command "{}" success!'.format(command))
        else:
            printw('Command "{}" failure on some nodes!'.format(command))
        return state


    '''
    Deploy data on the local drive of a node. We require:
    datalist the files/directories to deploy, as a list of string filenames (which exist in <project root>/data/),
    deploy_mode the deploy-mode for the data. Determines whether we place data on the NFS mount, local disk, RAMdisk etc,
    skip value (if True, we skip copying data that already exists in a particular node's local drive),
    subpath the extra path to append to the rsync target location
    retries for trying to deploy the application. If we fail, we first sleep retry_sleep_time before retrying.
    '''
    def deploy_data(self, datalist, deploy_mode, skip, subpath='', retries=5, retry_sleep_time=5):
        dmode = DeployMode.interpret(deploy_mode) if isinstance(deploy_mode, str) else deploy_mode
        dlist = listdatalist if isinstance(datalist, list) else [datalist]
        from deploy.deploy import _deploy_data_internal
        for x in range(retries):
            if _deploy_data_internal(dlist, dmode, skip, subpath=subpath):
                return True
            time.sleep(retry_sleep_time)
        return False


    # Print method to print to stderr
    def eprint(self, *args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)
