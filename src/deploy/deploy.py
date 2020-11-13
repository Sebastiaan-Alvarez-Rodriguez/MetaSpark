# This file handles argument parsing for deployments,
# as well as actual deployment.

import argparse
import os
import socket

from config.meta import cfg_meta_instance as metacfg
from remote.reservation import Reserver
import remote.util.ip as ip
from util.executor import Executor
import util.location as loc
import util.fs as fs
from util.printer import *
import util.time as tm
import util.ui as ui


# Deployment execution on remote
def _deploy_internal(jarfile, mainclass, args, extra_jars, driver_jvmargs, executor_jvmargs):
    print('Connected!')
    scriptloc = fs.join(loc.get_spark_bin_dir(), 'spark-submit')

    try:
        reserver = Reserver.load()
        master_url = reserver.deployment.master_url
    except FileNotFoundError as e:
        if not silent: printe('No reservation found on remote. Cannot run!')
        return False
    except Exception as e:
        if not silent: printe('Reservation file found, no longer active')
        return False

    timestamp = tm.timestamp('%Y-%m-%d_%H:%M:%S.%f')
    fs.mkdir(loc.get_metaspark_results_dir(), timestamp)

    driver_opts = '-Dlog4j.configuration=file:{} -Doutputlog={} -Dfile={}'.format(
        fs.join(loc.get_metaspark_log4j_conf_dir(),'driver_log4j.properties'),
        fs.join(loc.get_metaspark_results_dir(), timestamp, 'spark.log'),
        fs.join(loc.get_metaspark_results_dir(), timestamp, 'out.res')
    )
    driver_jvmargs = fs.join(loc.get_metaspark_results_dir(), timestamp, 'out.res')
    executor_jvmargs = fs.join(loc.get_metaspark_results_dir(), timestamp, 'out.res')
    print('Output log can be found at {}'.format(fs.join(loc.get_metaspark_results_dir(), timestamp)))
    
    extra_jars = ','.join(['file:'+fs.join(loc.get_metaspark_jar_dir(), x) for x in extra_jars.split(' ')])
    # TODO: Is this correct? 
    #  Option 1: https://stackoverflow.com/questions/37887168/how-to-pass-environment-variables-to-spark-driver-in-cluster-mode-with-spark-sub
    #  Option 2 (current): https://intellipaat.com/community/6625/how-to-pass-d-parameter-or-environment-variable-to-spark-job
    #  Option 3: https://stackoverflow.com/questions/46564970/log4j2-store-and-use-variables-lookup-values
    # Might use environment trick... However, it is not easy to have environment changes flow to allocated nodes!
    command = '{}\
    --driver-java-options "{}" \
    --class {} \
    --jars "{}" \
    --master {} \
    --deploy-mode cluster {} {}'.format(
        scriptloc,
        driver_opts,
        mainclass,
        extra_jars,
        master_url,
        fs.join(loc.get_metaspark_jar_dir(), jarfile),
        args)
    print('Executing command: {}'.format(command))
    status = os.system(command) == 0
    if status:
        prints('Deployment was successful!')
    else:
        printe('There were errors during deployment.')
    return status

def _deploy(jarfile, mainclass, args, extra_jars, driver_jvmargs, executor_jvmargs):
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

    print('Synchronizing jars to server...')
    command = 'rsync -az {} {}:{}'.format(loc.get_metaspark_jar_dir(), metacfg.ssh.ssh_key_name, loc.get_remote_metaspark_dir())
    command+= ' --exclude '+' --exclude '.join([
        '.git',
        '__pycache__'])
    if os.system(command) == 0:
        prints('Export success!')
    else:
        printe('Export failure!')
        return False

    program = '{} {} --internal --args \'{}\' --jars \'{}\' --driver_jvmargs \'{}\' --executor_jvmargs \'{}\''.format(
        jarfile, mainclass, args, extra_jars, driver_jvmargs, executor_jvmargs)

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
    deployparser.add_argument('--args', nargs='+', metavar='argument', help='Arguments to pass on to your jarfile')
    deployparser.add_argument('--jars', nargs='+', metavar='argument', help='Extra jars to pass along your jarfile')
    deployparser.add_argument('--executor_jvmargs', nargs='+', metavar='argument', help='Arguments to pass on to JVM in executors')
    deployparser.add_argument('--driver_jvmargs', nargs='+', metavar='argument', help='Arguments to pass on to JVM in the driver')
    
    deployparser.add_argument('--internal', help=argparse.SUPPRESS, action='store_true')


# Return True if we found arguments used from this subparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function 
def deploy_args_set(args):
    return args.command == 'deploy'


# Processing of deploy commandline args occurs here
def deploy(parser, args):
    jarfile = args.jarfile
    mainclass = args.mainclass
    jargs = ' '.join(args.args) if args.args != None else ''
    extra_jars = ' '.join(args.jars) if args.jars != None else ''

    driver_jvmargs = ' '.join(args.driver_jvmargs) if args.driver_jvmargs != None else ''
    executor_jvmargs = ' '.join(args.executor_jvmargs) if args.executor_jvmargs != None else ''

    if args.internal:
        return _deploy_internal(jarfile, mainclass, jargs, extra_jars, driver_jvmargs, executor_jvmargs)
    else:
        return _deploy(jarfile, mainclass, jargs, extra_jars, driver_jvmargs, executor_jvmargs)