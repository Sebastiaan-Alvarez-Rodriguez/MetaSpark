# Code in this file boots up a Spark cluster

import socket

from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.executor import Executor
from util.printer import *


# Boots master on given node.
# Note: Spark works with Daemons, so expect to return quickly,
# probably even before the master is actually ready
def boot_master(node, port=7077, webui_port=2205, debug_mode=False):
    scriptloc = fs.join(loc.get_spark_sbin_dir(), 'start-master.sh')

    cmd = 'ssh {} "{} --host {} --port {} --webui-port {} {}"'.format(
        node,
        scriptloc,
        node,
        port,
        webui_port,
        '> /dev/null 2>&1' if not debug_mode else ''
    )

    executor = Executor(cmd, shell=True)
    retval = executor.run_direct() == 0
    if retval:
        printc('MASTER ready on spark://{}:{} (webui-port: {})'.format(node, port, webui_port), Color.CAN)
    return retval

'''
Boots a slave.
Note: Spark works with Daemons, so expect to return quickly,
probably even before the slave is actually ready

Provide
    node:           ip/hostname of node to boot this worker
    master_node:    ip/hostname of master
    master_port:    port for master
    procs_per_node: Amount of slaves to spawn on this node
    debug_mode:     True if we must print debug info, False otherwiseexecute
    deploy_mode:    Determines where we should place slave work-dir: E.g. on local SSD, slow NFS-mount, etc
    execute:        Function executes boot command and returns state_ok (a bool) if True,
                    otherwise returns Executor to boot slave, which you have to execute to deploy slave
'''
def boot_slave(node, master_node, master_port=7077, debug_mode=False, deploy_mode=DeployMode.STANDARD, execute=True):
    scriptloc = fs.join(loc.get_spark_sbin_dir(), 'start-slave.sh')
    master_url = 'spark://{}:{}'.format(master_node, master_port)

    # workdir = fs.join(loc.get_node_local_dir(), lid)
    # fs.rm(workdir, ignore_errors=True)
    # fs.mkdir(workdir)

    # port = master_port+lid #Adding lid ensures we use different ports when sharing a node
    # webui_port = 8080+lid

    if debug_mode: print('Spawning worker on {}'.format(node))

    cmd = 'ssh {} "{} {} {} {}"'.format(
            node,
            scriptloc,
            master_url,
            '--work-dir {}'.format(loc.get_spark_work_dir(deploy_mode)),
            '> /dev/null 2>&1' if not debug_mode else ''
        )

    executor = Executor(cmd, shell=True)
    if execute:
        retval = executor.run_direct() == 0
        if debug_mode:
            if retval:
                prints('Booted slave on {}!'.format(node))
            else:
                printe('Failed to boot slave on {}'.format(node))
        return retval
    return executor

'''
Boots multiple slaves in parallel.

Provide
    nodes:          ip/hostname list of nodes to boot workers on
    master_node:    ip/hostname of master
    master_port:    port for master
    debug_mode:     True if we must print debug info, False otherwise
    deploymode:     Denotes where we locate the slave work-dir: E.g. on NFS-mount for debugging (slow), on local disk for each node, etc
'''
def boot_slaves(nodes, master_node, master_port=7077, debug_mode=False, deploy_mode=DeployMode.STANDARD):
    executors = []
    for node in nodes:
        executors.append(boot_slave(node, master_node, master_port=master_port, deploy_mode=deploy_mode, debug_mode=debug_mode, execute=False))
    Executor.run_all(executors)
    return Executor.wait_all(executors)