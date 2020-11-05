# This file contains all relevant paths for MetaSpark to function
# Here, we chose for a function-call approach instead of
# a global object, as we don't have to maintain state here.

import util.fs as fs
from config.meta import cfg_meta_instance as metacfg

#################### MetaSpark directories ####################
def get_metaspark_dep_dir():
    return fs.join(fs.abspath(), 'deps')

def get_metaspark_experiment_dir():
    return fs.join(fs.abspath(), 'experiments')

def get_metaspark_results_dir():
    return fs.join(fs.abspath(), 'results')

def get_metaspark_logs_dir():
    return fs.join(fs.abspath(), 'logs')

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

#################### Remote directories ####################
def get_remote_metaspark_parent_dir():
    return metacfg.ssh.remote_metaspark_dir

def get_remote_metaspark_dir():
    return fs.join(get_remote_metaspark_parent_dir(), fs.basename(fs.abspath()))


#################### Node directories ####################
# Because we  will use client logging using plan 2, this should change
def get_node_local_dir():
    return '/local/{}/'.format(metacfg.ssh.ssh_user_name)