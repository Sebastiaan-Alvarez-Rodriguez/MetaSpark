# This file contains function handles to control spawning and destroying entire clusters,
# to deploy applications and data on running clusters,
# and to clean up after application execution.

from enum import Enum
import os
import sys
import time

from config.meta import cfg_meta_instance as metacfg
from remote.reserver import reservation_manager
from remote.util.deploymode import DeployMode
from util.executor import Executor
import util.fs as fs
import util.location as loc
from util.printer import *
import util.printer as up

class MetaDeployState(Enum):
    '''Possible deployment states'''
    INIT = 0     # Not working yet
    BUSY = 1     # Working
    COMPLETE = 2 # Finished, with success
    FAILED = 3   # Finished, with failure


class MetaDeploy(object):
    '''Object to dynamically pass to meta deployment setups'''

    def __init__(self):
        self._reservation_numbers = list()
        self.index = -1
        self.amount = -1

    def set_idx_amt(self, index, amount):
        self.index = index
        self.amount = amount

    def print(self, *args, **kwargs):
        up.print('[{}/{}] '.format(self.index, self.amount), end='')
        up.print(*args, **kwargs)

    def printw(self, string, color=Color.YEL, **kwargs):
        up.printw('[{}/{}] {}'.format(self.index, self.amount, string), color, **kwargs)

    def printe(self, string, color=Color.RED, **kwargs):
        up.printe('[{}/{}] {}'.format(self.index, self.amount, string), color, **kwargs)

    def prints(self, string, color=Color.GRN, **kwargs):
        up.prints('[{}/{}] {}'.format(self.index, self.amount, string), color, **kwargs)

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
    Returns allocation on success, None on failure
    '''
    def cluster_start(self, time_to_reserve, config_filename, debug_mode, deploy_mode, launch_spark=True, retries=5, retry_sleep_time=5):
        object_deploymode = DeployMode.interpret(deploy_mode) if isinstance(deploy_mode, str) else deploy_mode
        reservation = None
        for x in range(retries):
            try:
                if reservation == None:
                    from main import _start_cluster
                    reservation = _start_cluster(time_to_reserve, config_filename)
                if launch_spark:
                    from main import _start_spark_on_cluster
                    if not _start_spark_on_cluster(reservation, debug_mode, object_deploymode):
                        continue
                self._reservation_numbers.append(reservation.number)
                return reservation
            except Exception as e:
                printe('Error during cluster start: ', end='')
                raise e
                time.sleep(retry_sleep_time)
        return None

    '''
    We stop a cluster using this function.
    We try to stop the cluster for retries retries. If we fail, we first sleep retry_sleep_time before retrying.
    '''
    def cluster_stop(self, reservation, silent=False, retries=5, retry_sleep_time=5):
        from main import stop
        number = reservation.number
        for x in range(retries):
            if stop([number], silent=silent):
                self._reservation_numbers.remove(number)
                return True
            time.sleep(retry_sleep_time)
        return False


    # Remove junk generated during each run. Please use this between runs, not in a run
    def clean_junk(self, reservation, deploy_mode=None, fast=False, datadir=None):
        if deploy_mode == None: # We don't know where to clean. Clean everywhere
            workdirs = ' '.join([loc.get_spark_work_dir(val) for val in DeployMode])
            fast = False # we don't know the node numbers!
        else:
            object_deploymode = DeployMode.interpret(deploy_mode) if isinstance(deploy_mode, str) else deploy_mode
            workdirs = loc.get_spark_work_dir(object_deploymode)

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

            for x in reservation.deployment.nodes:
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
    reservation object for a cluster we have spawned (and this cluster must run Spark of course)
    jarfile name (which exists in <project root>/jars/),
    mainclass inside the jarfile,
    args for the jar (can be None),
    extra_jars (which exist in <project root>/jars/) to submit alongside the jarfile (can be None),
    submit_opts (str) extra options for spark-submit (for advanced users),
    no_resultdir (bool) to indicate whether we should skip making a result directory or not,
    flamegraph_time (str) to indicate whether we want to record data for a flamegraph (looks like 30s, 2m, 4h, can be None)
    retries for trying to deploy the application. If we fail, we first sleep retry_sleep_time before retrying.
    '''
    def deploy_application(self, reservation, jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir, retries=5, retry_sleep_time=5):
        from deploy.deploy import _deploy_application
        for x in range(retries):
            if _deploy_application(reservation.number, jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir):
                return True
            time.sleep(retry_sleep_time)
        return False

    '''
    Deploy an application (which is not a Spark application) on all nodes to run in parallel.
    Useful to e.g. generate data
    '''
    def deploy_nonspark_application(self, reservation, command):
        if not reservation.validate():
            printe('Reservation no longer valid. Cannot execute non-spark command: {}'.format(command))
            return False
        executors = []
        for host in reservation.deployment.nodes:
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
    reservation_or_number the reservation object, or int reservation number to use
    datalist the files/directories to deploy, as a list of string filenames (which exist in <project root>/data/),
    deploy_mode the deploy-mode for the data. Determines whether we place data on the NFS mount, local disk, RAMdisk etc,
    skip value (if True, we skip copying data that already exists in a particular node's local drive),
    subpath the extra path to append to the rsync target location
    retries for trying to deploy the application. If we fail, we first sleep retry_sleep_time before retrying.
    '''
    def deploy_data(self, reservation_or_number, datalist, deploy_mode, skip, subpath='', retries=5, retry_sleep_time=5):
        dmode = DeployMode.interpret(deploy_mode) if isinstance(deploy_mode, str) else deploy_mode
        dlist = listdatalist if isinstance(datalist, list) else [datalist]
        from deploy.deploy import _deploy_data
        for x in range(retries):
            if _deploy_data(reservation_or_number, dlist, dmode, skip, subpath=subpath):
                return True
            time.sleep(retry_sleep_time)
        return False


    def deploy_flamegraph(self, reservation_or_number, flame_graph_duration='30s', only_master=False, only_worker=False):
        if flame_graph_duration != None:
            from deploy.deploy import _flamegraph
            _flamegraph(reservation_or_number, flame_graph_duration, only_master, only_worker)


    # Print method to print to stderr
    def eprint(self, *args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)
