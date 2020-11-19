# This file handles argument parsing for deployments,
# as well as actual deployment.

import argparse
import glob
import os
import socket

from config.meta import cfg_meta_instance as metacfg
import dynamic.experiment as exp
from remote.reservation import Reserver
import remote.util.ip as ip
from util.executor import Executor
import util.location as loc
import util.fs as fs
from util.printer import *
import util.time as tm
import util.ui as ui

def _args_replace(args, timestamp, no_result=False):    
    tmp1 = args if no_result else args.replace('[[RESULTDIR]]', fs.join(loc.get_metaspark_results_dir(), timestamp))
    return tmp1.replace('[[DATADIR]]', loc.get_node_local_ssd_dir())


def _deploy_application_internal(jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir):
    if args == None:
        args = ''
    if extra_jars == None:
        extra_jars = ''
    if submit_opts == None:
        submit_opts = ''
    print('Connected!')
    scriptloc = fs.join(loc.get_spark_bin_dir(), 'spark-submit')

    try:
        reserver = Reserver.load()
        master_url = reserver.deployment.master_url
    except FileNotFoundError as e:
        printe('No reservation found on remote. Cannot run!')
        return False
    except Exception as e:
        printe('Reservation file found, no longer active')
        return False

    if no_resultdir:
        driver_opts = '-Dlog4j.configuration=file:{}'.format(fs.join(loc.get_metaspark_log4j_conf_dir(),'driver_log4j.properties'))
        timestamp = None
    else:
        timestamp = tm.timestamp('%Y-%m-%d_%H:%M:%S.%f')
        fs.mkdir(loc.get_metaspark_results_dir(), timestamp)
        driver_opts = '-Dlog4j.configuration=file:{} -Doutputlog={}'.format(
            fs.join(loc.get_metaspark_log4j_conf_dir(),'driver_log4j.properties'),
            fs.join(loc.get_metaspark_results_dir(), timestamp, 'spark.log'))
        print('Output log can be found at {}'.format(fs.join(loc.get_metaspark_results_dir(), timestamp)))

    args = _args_replace(args, timestamp, no_result=no_resultdir)
    submit_opts = _args_replace(submit_opts, timestamp, no_result=no_resultdir)

    
    if len(extra_jars) > 0:
        extra_jars = ','.join([fs.join(loc.get_metaspark_jar_dir(), x) for x in extra_jars.split(' ')])+','
    command = '{}\
    --driver-java-options "{}" \
    --class {} \
    --jars "{}" \
    --conf spark.driver.extraClassPath={} \
    --conf spark.executor.extraClassPath={} \
    {} \
    --master {} \
    --deploy-mode cluster {} {}'.format(
        scriptloc,
        driver_opts,
        mainclass,
        extra_jars,
        fs.join(loc.get_metaspark_jar_dir(), jarfile),
        fs.join(loc.get_metaspark_jar_dir(), jarfile),
        submit_opts,
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


def _deploy_application(jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir):
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
    command+= ' --exclude '+' --exclude '.join(['.git', '__pycache__'])
    if os.system(command) == 0:
        prints('Export success!')
    else:
        printe('Export failure!')
        return False

    program = '{} {} --internal --args \'{}\' --jars \'{}\' --opts \'{}\''.format(
        jarfile, mainclass, args, extra_jars, submit_opts)
    program += '--no-resultdir' if no_resultdir else '' 

    command = 'ssh {} "python3 {}/main.py deploy application {}"'.format(
    metacfg.ssh.ssh_key_name,
    loc.get_remote_metaspark_dir(),
    program)
    print('Connecting using key "{}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0


def _deploy_data_internal(datalist, skip):
    print('Synchronizing data to local nodes...')
    data = ' '.join([fs.join(loc.get_metaspark_data_dir(), fs.basename(x)) for x in datalist])
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
        command = 'rsync -az {} {}:{}'.format(data, host, loc.get_node_local_ssd_dir())
        command+= ' --exclude '+' --exclude '.join(['.git', '__pycache__'])
        if skip:
            command+= '--ignore-existing'
        executors.append(Executor(command, shell=True))
    Executor.run_all(executors)
    state = Executor.wait_all(executors, stop_on_error=False)
    if state:
        prints('Export success!')
    else:
        printe('Export failure!')
    return state
    

def _deploy_data(datalist, skip):
    for location in datalist:
        glob_locs = glob.glob(location)
        for glob_loc in glob_locs:
            if not fs.exists(glob_loc):
                printe('Path "{}" does not exist'.format(glob_loc))
                return False
    
    print('Synchronizing data to server...')
    data = ' '.join(datalist)
    command = 'rsync -az {} {}:{}'.format(data, metacfg.ssh.ssh_key_name, loc.get_remote_metaspark_data_dir())
    command+= ' --exclude '+' --exclude '.join(['.git', '__pycache__'])
    if skip:
        command+= '--ignore-existing'
    if os.system(command) == 0:
        print('Export success!')
    else:
        printe('Export failure!')
        return False
    
    program = '{} --internal {}'.format(data, '--skip' if skip else '')
    command = 'ssh {} "python3 {}/main.py deploy data {}"'.format(metacfg.ssh.ssh_key_name, loc.get_remote_metaspark_dir(), program)
    print('TMP: command: {}'.format(command))
    print('Connecting using key "{}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0


def _deploy_meta():
    try:
        experiments = exp.get_experiments()
    except RuntimeError as e:
        printe('Could not find an experiment to run. Please make an experiment in {}. See the README.md for more info.'.format(loc.get_metaspark_experiments_dir()))
        return False
    for idx, x in enumerate(experiments):
        print('Starting experiment {}'.format(idx))
        if x.start():
            print('Experiment {} completed successfully'.format(idx))
        else:
            print('There were some problems during experiment {}!'.format(idx))
        x.stop()
        print('Experiment {} stopped'.format(idx))
    return True

# Register 'deploy' subparser modules
def subparser(subparsers):
    deployparser = subparsers.add_parser('deploy', help='Deploy applications/data (use deploy -h to see more...)')
    subsubparsers = deployparser.add_subparsers(help='Subsubcommands', dest='subcommand')
    
    deployapplparser = subsubparsers.add_parser('application', help='Deploy applications (use deploy start -h to see more...)')
    deployapplparser.add_argument('jarfile', help='Jarfile to deploy')
    deployapplparser.add_argument('mainclass', help='Main class of jarfile')
    deployapplparser.add_argument('--args', nargs='+', metavar='argument', help='Arguments to pass on to your jarfile')
    deployapplparser.add_argument('--jars', nargs='+', metavar='argument', help='Extra jars to pass along your jarfile')
    deployapplparser.add_argument('--opts', nargs='+', metavar='argument', help='Extra arguments to pass on to spark-submit')    
    deployapplparser.add_argument('--no-resultdir', dest='no_resultdir', help='Do not make a resultdirectory in <project root>/results/ for this deployment', action='store_true')
    deployapplparser.add_argument('--internal', help=argparse.SUPPRESS, action='store_true')

    deploydataparser = subsubparsers.add_parser('data', help='Deploy data (use deploy start -h to see more...)')
    deploydataparser.add_argument('data', nargs='+', metavar='file', help='Files to place on reserved nodes local drive')
    deploydataparser.add_argument('--skip', help='Skip data if already found on remote', action='store_true')
    deploydataparser.add_argument('--internal', help=argparse.SUPPRESS, action='store_true')
    
    deploymetaparser = subsubparsers.add_parser('meta', help='Deploy applications with all variations of given parameters')
    deploymetaparser.add_argument('--internal', help=argparse.SUPPRESS, action='store_true')

    return deployparser, deployapplparser, deploymetaparser, deploydataparser


# Return True if we found arguments used from this subparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function 
def deploy_args_set(args):
    return args.command == 'deploy'


# Processing of deploy commandline args occurs here
def deploy(parsers, args):
    deployparser, deployapplparser, deploymetaparser, deploydataparser = parsers
    if args.subcommand == 'application':
        jarfile = args.jarfile
        mainclass = args.mainclass
        jargs = ' '.join(args.args) if args.args != None else ''
        extra_jars = ' '.join(args.jars) if args.jars != None else ''
        submit_opts = ' '.join(args.opts) if args.opts != None else ''
        if args.internal:
            return _deploy_application_internal(jarfile, mainclass, jargs, extra_jars, submit_opts, args.no_resultdir)
        else:
            return _deploy_application(jarfile, mainclass, jargs, extra_jars, submit_opts, args.no_resultdir)
    elif args.subcommand == 'data':
        if args.internal:
            return _deploy_data_internal(args.data, args.skip)
        else:
            return _deploy_data(args.data, args.skip)
    elif args.subcommand == 'meta':
        _deploy_meta()

    else:
        deployparser.print_help()