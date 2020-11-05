# This file handles argument parsing for deployments,
# as well as actual deployment.

import argparse
import os
import socket

from config.meta import cfg_meta_instance as metacfg
import remote.util.ip as ip
from util.executor import Executor
import util.location as loc
import util.fs as fs
from util.printer import *
import util.time as tm
import util.ui as ui

# Post-deployment gathering of logs
def _deploy_collect(timestamp):
    for lid in (x for x in fs.ls(loc.get_node_local_dir(), only_dirs=True) if x.isnumeric()):
        tmp = fs.join(loc.get_node_local_dir(), lid)
        options = [x for x in fs.ls(tmp) if x.startswith('driver-')]
        needed = sorted(options, key=lambda x: int(x.split('-')[1]))[0]
        fs.mv(fs.join(tmp, needed), fs.join(loc.get_metaspark_logs_dir(), timestamp, socket.gethostname()+'_'+lid))
    return True

# Deployment execution on remote
def _deploy_internal(jarfile, mainclass, master_url, args):
    print('Connected!')
    scriptloc = fs.join(loc.get_spark_bin_dir(), 'spark-submit')

    timestamp = tm.timestamp('%Y-%m-%d_%H:%M:%S.%f')
    fs.mkdir(loc.get_metaspark_logs_dir(), timestamp)

    driver_opts = '-Dlog4j.configuration=file:{} -Doutputlog={}'.format(
        fs.join(loc.get_metaspark_log4j_conf_dir(),'driver_log4j.properties'),
        fs.join(loc.get_metaspark_logs_dir(), timestamp, 'spark.log')
        )
    print('Output log can be found at {}'.format(fs.join(loc.get_metaspark_logs_dir(), timestamp)))
    
    command = '{}\
    --driver-java-options "{}"\
    --class {}\
    --master {}\
    --deploy-mode cluster {} {}'.format(
        scriptloc,
        driver_opts,
        mainclass,
        master_url,
        fs.join(loc.get_metaspark_jar_dir(), jarfile),
        args)
    status = os.system(command) == 0
    if status:
        prints('Deployment was successful!')
    else:
        printe('There were errors during deployment.')
    print('')
    print('Gathering log results')
    # TODO: Gather node ips
    executors = [Executor('ssh {} "python3 {}/main.py deploy . . . --deploy_collect {}"'.format(node, locfs.abspath(), timestamp)) for node in nodes]

    Executor.run_all(executors)
    status2 = Executor.wait_all(executors, stop_on_error=False)
    if status2:
        print('Exported logs to {}!'.format(fs.join(loc.get_metaspark_logs_dir(), timestamp)))
    else:
        print('Export failures detected, got as many logs as possible!')
    # '/local/hpcl1910/1/driver-20201031183111-0000/stdout'
    return status and status2

def _deploy(jarfile, mainclass, master_url, args):
    fs.mkdir(loc.get_metaspark_jar_dir(), exist_ok=True)
    if not fs.isfile(loc.get_metaspark_jar_dir(), jarfile):
        printw('Provided jarfile "{}" not found at "{}"'.format(jarfile, loc.get_metaspark_jar_dir()))
        while True:
            options = [fs.basename(x) for x in fs.ls(loc.get_metaspark_jar_dir(), only_files=True, full_paths=True) if x.endswith('.jar')]
            if len(options)== 0: print('Note: {} seems to be an empty directory...'.format(loc.get_metaspark_jar_dir()))
            idx = ui.ask_pick('Pick a jarfile: ', ['Rescan {}'.format(loc.get_metaspark_jar_dir())]+options)
            if idx == 0:
                continue
            else:
                jarfile = options[idx-1]
                break
    command = 'rsync -az {} {}:{}'.format(loc.get_metaspark_jar_dir(), metacfg.ssh.ssh_key_name, fs.join(loc.get_remote_metaspark_dir(), 'jars'))
    command+= ' --exclude '+' --exclude '.join([
        '.git',
        '__pycache__'])
    if os.system(command) == 0:
        prints('Export success!')
    else:
        printe('Export failure!')
        return False

    program = '{} {} {} --deploy_internal --args {}'.format(jarfile, mainclass, master_url, args)

    command = 'ssh {} "python3 {}/main.py deploy {}"'.format(
    metacfg.ssh.ssh_key_name,
    loc.get_remote_metaspark_dir(),
    program)
    print('Connecting using key "{}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0


# Register 'deploy' subparser modules
def subparser(subparsers):
    deployparser = subparsers.add_parser('deploy', help='Deploy applications (use deploy -h to see more...)')
    deployparser.add_argument('jarfile', help='Jarfile to deploy')
    deployparser.add_argument('mainclass', help='Main class of jarfile')
    deployparser.add_argument('master_url', help='Master url for cluster')
    deployparser.add_argument('--args', nargs='*', help='Arguments to pass on to your jarfile')
    deployparser.add_argument('--deploy_internal', help=argparse.SUPPRESS, action='store_true')
    deployparser.add_argument('--deploy_collect', nargs=1, help=argparse.SUPPRESS)
    

# Return True if we found arguments used from this subparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function 
def deploy_args_set(args):
    return args.command == 'deploy'
# Processing of deploy commandline args occurs here
def deploy(parser, args):
    jarfile = args.jarfile
    mainclass = args.mainclass
    master_url = args.master_url
    jargs = ' '.join(args.args) if args.args != None else ''

    if args.deploy_collect:
        return _deploy_collect(args.deploy_collect[0])
    elif args.deploy_internal:
        return _deploy_internal(jarfile, mainclass, master_url, jargs)
    else:
        return _deploy(jarfile, mainclass, master_url, jargs)