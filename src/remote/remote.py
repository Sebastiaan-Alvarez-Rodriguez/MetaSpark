# Code in this file is the entrypoint for prun calls.
# run_server() and run_client() are called as soon as allocation is done.
# Goal of this file is to guide data processing on server and client nodes.


import socket
import time

import config.cluster as clr
import remote.util.identifier as idr
import remote.util.ip as ip
# from remote.util.syncer import Syncer
import util.fs as fs
import util.location as loc
from util.executor import Executor
from util.printer import *

# Boots master. Spark works with Daemons, so expect to return quickly from this function
def boot_master(cluster_cfg, port, debug_mode):
    lid = idr.identifier_local()
    spark_conf_dir = loc.get_spark_conf_dir() #"${SPARK_CONF_DIR:-"${SPARK_HOME}/conf"}"


    scriptloc = fs.join(loc.get_spark_sbin_dir(), 'start-master.sh')

    spark_webui_port = 2205

    cmd = '{} --host {} --port {} --webui-port {}'.format(scriptloc, ip.master_address(cluster_cfg.infiniband), port, spark_webui_port)
    cmd += '2>&1 > devnull' if not debug_mode else ''
    executor = Executor(cmd, shell=True)
    retval = executor.run_direct() == 0
    printc('MASTER ready on spark://{}:{}'.format(ip.master_address(cluster_cfg.infiniband), port), Color.CAN)
    return retval


# Boots a slave. Spark works with Daemons, so expect to return quickly from this function
def boot_slave(cluster_cfg, master_port, debug_mode):
    gid = idr.identifier_global()
    lid = idr.identifier_local()
    scriptloc = fs.join(loc.get_spark_sbin_dir(), 'start-slave.sh')
    master_url = 'spark://{}:{}'.format(ip.master_address(cluster_cfg.infiniband), master_port)

    workdir = fs.join(loc.get_node_local_dir(), lid)
    fs.rm(workdir, ignore_errors=True)
    fs.mkdir(workdir)

    port = master_port+lid #Adding lid ensures we use different ports when sharing a node
    webui_port = 8080+lid

    time.sleep(5)
    if debug_mode: print('Slave {}:{} connecting to {}, standing by on port {}'.format(gid, lid, master_url, port))
    fqdn = socket.getfqdn()
    
    cmd = '{} {} --cores 1 --memory 1024M --work-dir {} --host {} --port {} --webui-port {}'.format(scriptloc, master_url, workdir, fqdn, port, webui_port)
    cmd += '2>&1 > devnull' if not debug_mode else ''
    
    executor = Executor(cmd, shell=True)
    return executor.run_direct() == 0


# Run with debug_mode (True/False) and the name of the clusterconfig to load
def run(configname, debug_mode):
    cluster_cfg = clr.load_cluster_config(configname)
    port = 7077
    gid = idr.identifier_global()
    lid = idr.identifier_local()

    status = boot_master(cluster_cfg, port, debug_mode) if gid == 0 else boot_slave(cluster_cfg, port, debug_mode)
    if not status:
        printe('Error booting {}'.format('Master' if gid==0 else 'slave {}:{}'.format(gid, lid)))

    if gid == 0:
        print('')
    while True: # Sleep forever, 1 minute at a time
        time.sleep(60)