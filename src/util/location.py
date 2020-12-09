# This file contains all relevant paths for MetaSpark to function
# Here, we chose for a function-call approach instead of
# a global object, as we don't have to maintain state here.

import util.fs as fs
from config.meta import cfg_meta_instance as metacfg
from remote.util.deploymode import DeployMode

#################### MetaSpark directories ####################
def get_metaspark_dep_dir():
    return fs.join(fs.abspath(), 'deps')

def get_metaspark_data_dir():
    return fs.join(fs.abspath(), 'data')

def get_metaspark_experiments_dir():
    return fs.join(fs.abspath(), 'experiments')

def get_metaspark_results_dir():
    return fs.join(fs.abspath(), 'results')

def get_metaspark_graphs_dir():
    return fs.join(fs.abspath(), 'graphs')

def get_metaspark_conf_dir():
    return fs.join(fs.abspath(), 'conf')

def get_metaspark_cluster_conf_dir():
    return fs.join(get_metaspark_conf_dir(), 'cluster')

def get_metaspark_log4j_conf_dir():
    return fs.join(get_metaspark_conf_dir(), 'log4j')

def get_metaspark_jar_dir():
    return fs.join(fs.abspath(), 'jars')

#################### Spark directories ####################
def get_spark_dir():
    return fs.join(get_metaspark_dep_dir(), 'spark')

def get_spark_bin_dir():
    return fs.join(get_spark_dir(), 'bin')

def get_spark_sbin_dir():
    return fs.join(get_spark_dir(), 'sbin')

def get_spark_conf_dir():
    return fs.join(get_spark_dir(), 'conf')

def get_spark_logs_dir():
    return fs.join(get_spark_dir(), 'logs')

def get_spark_work_dir(deploy_mode):
    if deploy_mode == DeployMode.STANDARD:
        return fs.join(get_spark_dir(), 'work')
    elif deploy_mode == DeployMode.LOCAL:
        return fs.join(get_node_local_dir(), 'work')
    elif deploy_mode == DeployMode.LOCAL_SSD:
        return fs.join(get_node_local_ssd_dir(), 'work')
    elif deploy_mode == DeployMode.RAM:
        return fs.join(get_node_ram_dir(), 'work')
    else:
        raise RuntimeError('Could not find a workdir destination for deploymode: {}'.format(deploy_mode))

#################### Remote directories ####################
def get_remote_metaspark_parent_dir():
    return metacfg.ssh.remote_metaspark_dir

def get_remote_metaspark_dir():
    return fs.join(get_remote_metaspark_parent_dir(), fs.basename(fs.abspath()))

def get_remote_metaspark_conf_dir():
    return fs.join(get_remote_metaspark_dir(), 'conf')

def get_remote_metaspark_jar_dir():
    return fs.join(get_remote_metaspark_dir(), 'jars')

def get_remote_metaspark_data_dir():
    return fs.join(get_remote_metaspark_dir(), 'data')

#################### Node directories ####################
# Because we will use client logging using plan 2, this should change
def get_node_local_dir():
    return '/local/{}/'.format(metacfg.ssh.ssh_user_name)

# Faster dir than /local/. This is a local dir mapped on an SSD!
# Note: Not all machines have this directory. Others have it but deny permission,
# because the particular node has no SSD.
# Need a special allocation command to get only nodes with SSD disks.
def get_node_local_ssd_dir():
    return '/local-ssd/{}/'.format(metacfg.ssh.ssh_user_name)

def get_node_ram_dir():
    return '{}/{}/'.format(get_node_raw_ram_dir(), metacfg.ssh.ssh_user_name)

def get_node_raw_ram_dir():
    return '/dev/shm/'.format(metacfg.ssh.ssh_user_name)

def get_node_data_dir(deploy_mode):
    if deploy_mode == DeployMode.STANDARD:
        return get_metaspark_data_dir()
    elif deploy_mode == DeployMode.LOCAL:
        return fs.join(get_node_local_dir(), 'data')
    elif deploy_mode == DeployMode.LOCAL_SSD:
        return fs.join(get_node_local_ssd_dir(), 'data')
    elif deploy_mode == DeployMode.RAM:
        return fs.join(get_node_ram_dir(), 'data')
    else:
        raise RuntimeError('Could not find a datadir destination for deploymode: {}'.format(deploy_mode))
