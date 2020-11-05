# Code in this file boots up a Spark cluster

import socket

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
    lid:            local id on the given node. Must be different for each spawn
    master_node:    ip/hostname of master
    master_port:    port for master
    debug_mode:     True if we must print debug info, False otherwise
    execute:        Function executes built command and returns state_ok (another bool) if True, returns command if False
'''
def boot_slave(node, lid, master_node, master_port=7077, debug_mode=False, execute=True):
    scriptloc = fs.join(loc.get_spark_sbin_dir(), 'start-slave.sh')
    master_url = 'spark://{}:{}'.format(master_node, master_port)

    workdir = fs.join(loc.get_node_local_dir(), lid)
    # fs.rm(workdir, ignore_errors=True)
    # fs.mkdir(workdir)

    port = master_port+lid #Adding lid ensures we use different ports when sharing a node
    webui_port = 8080+lid

    if debug_mode: print('Slave {}:{} connecting to {}, standing by on port {} (webui-port: {})'.format(gid, lid, master_url, port, webui_port))
    fqdn = socket.getfqdn()
    
    cmd = 'ssh {} "{} {} --cores 1 --memory 1024M --work-dir {} --host {} --port {} --webui-port {}"'.format(
            node,
            scriptloc,
            master_url,
            workdir,
            fqdn,
            port,
            webui_port,
            '2>&1 > devnull' if not debug_mode else ''
        )

    if execute:
        executor = Executor(cmd, shell=True)
        retval = executor.run_direct() == 0
        if debug:
            if retval:
                prints('Booted slave {}:{}!'.format(node, lid))
            else:
                printe('Failed to boot slave {}:{}!'.format(node, lid))
        return retval
    return cmd

'''
Boots multiple slaves in parallel.

Provide
    nodes:          ip/hostname list of nodes to boot workers on
    procs_per_node: Amount of processes to spawn
    master_node:    ip/hostname of master
    master_port:    port for master
    debug_mode:     True if we must print debug info, False otherwise
'''
def boot_slaves(nodes, procs_per_node, master_node, master_port=7077, debug_mode=False):
    executors = []
    for node in nodes:
        for x in range(procs_per_node):
            executors.append(Executor(boot_slave(node, x, master_node, master_port=master_port, debug_mode=debug_mode), shell=True))
    Executor.run_all(executors)
    return Executor.wait_all(executors)