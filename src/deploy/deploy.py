# This file handles argument parsing for deployments,
# as well as actual deployment.

import argparse
import glob
import os
import socket
import time

from config.meta import cfg_meta_instance as metacfg
import deploy.flamegraph as fg
import dynamic.experiment as exp
from remote.reservation import Reserver
from remote.util.deploymode import DeployMode
import remote.util.ip as ip
from util.executor import Executor
import util.location as loc
import util.fs as fs
from util.printer import *
import util.time as tm
import util.ui as ui

def _args_replace(args, timestamp, no_result=False):    
    tmp1 = args if no_result else args.replace('[[RESULTDIR]]', fs.join(loc.get_metaspark_results_dir(), timestamp))
    tmp1 = tmp1.replace('[[DATA-STANDARDDIR]]', loc.get_node_data_dir(DeployMode.STANDARD))
    tmp1 = tmp1.replace('[[DATA-LOCALDIR]]', loc.get_node_data_dir(DeployMode.LOCAL))
    tmp1 = tmp1.replace('[[DATA-LOCAL-SSDDIR]]', loc.get_node_data_dir(DeployMode.LOCAL_SSD))
    return tmp1.replace('[[DATA-RAMDIR]]', loc.get_node_data_dir(DeployMode.RAM))


def _deploy_application_internal(jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir, flame_graph=False, flame_graph_duration='30s'):
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

    if flame_graph:
        executors = []
        base_recordpath = fs.join(loc.get_metaspark_recordings_dir(), tm.timestamp('%Y-%m-%d_%H:%M:%S.%f'))
        fs.mkdir(base_recordpath, exist_ok=False)
        for host in reserver.deployment.nodes:
            flame_command = 'ssh {} "python3 {}/main.py deploy flamegraph -t {} -o {}"'.format(host, fs.abspath(), flame_graph_duration, base_recordpath)
            executors.append(Executor(flame_command, shell=True))
    print('Executing command: {}'.format(command))
    status = os.system(command) == 0
    if status:
        prints('Deployment was successful!')
    else:
        printe('There were errors during deployment.')

    if flame_graph:
        Executor.run_all(executors) # Connect to all nodes, start listening for correct pids
        print('Flamegraph reading set for {}. listening started...'.format(flame_graph_duration))
        Executor.wait_all(executors, stop_on_error=False)
    return status


def _flamegraph(flame_graph_duration, base_recordpath):
    try:
        reserver = Reserver.load()
    except FileNotFoundError as e:
        printe('No reservation found on remote. Cannot run!')
        return False
    except Exception as e:
        printe('Reservation file found, no longer active')
        return False

    designation = 'driver' if reserver.deployment.is_master() else 'worker'
    gid = reserver.deployment.get_gid()
    recordpath = fs.join(base_recordpath, designation+str(gid)+'.jfr')
    pid = None
    tries = 100
    while pid == None and tries > 0:
        pid = fg.find_proc_regex()
        tries -= 1
    if pid == None:
        import socket
        printw('{}: Unable to find jPID, skipping flamegraph'.format(socket.gethostname()))
        return False
    else:
        fg.launch_flightrecord(pid, recordpath, duration=flame_graph_duration)
        printc('Flight recording started, output will be at {}'.format(recordpath), Color.CAN)
    return True

def _deploy_application(jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir, flame_graph=False, flame_graph_duration='30s'):
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
    program += ' --no-resultdir' if no_resultdir else '' 
    program += ' --flamegraph {}'.format(flame_graph_duration) if flame_graph else ''
    command = 'ssh {} "python3 {}/main.py deploy application {}"'.format(
    metacfg.ssh.ssh_key_name,
    loc.get_remote_metaspark_dir(),
    program)
    print('Connecting using key "{}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0


def _deploy_data_internal(datalist, deploy_mode, skip, subpath=''):
    print('Synchronizing data to local nodes...')
    data = ' '.join(datalist)
    try:
        reserver = Reserver.load()
    except FileNotFoundError as e:
        printe('No reservation found on remote. Cannot run!')
        return False
    except Exception as e:
        printe('Reservation file found, no longer active')
        return False

    if deploy_mode == DeployMode.STANDARD: 
        # We already collected the data in our data dir on the NFS mount, so no need to copy again
        state = True
    else:
        target_dir = fs.join(loc.get_node_data_dir(deploy_mode), subpath)

        mkdir_executors = []
        executors = []
        for host in reserver.deployment.nodes:
            mkdir_executors.append(Executor('ssh {} "mkdir -p {}"'.format(host, target_dir), shell=True))

            command = 'rsync -az {} {}'.format(data, target_dir)
            command+= ' --exclude '+' --exclude '.join(['.git', '__pycache__', '*.crc'])
            if skip:
                command+= ' --ignore-existing'
            executors.append(Executor('ssh {} "{}"'.format(host, command), shell=True))
        Executor.run_all(mkdir_executors)
        state = Executor.wait_all(mkdir_executors, stop_on_error=False)
        Executor.run_all(executors)
        state &= Executor.wait_all(executors, stop_on_error=False)
    if state:
        prints('Data deployment success!')
    else:
        printw('Data deployment failure on some nodes!')
    return state
    

def _deploy_data(datalist, deploy_mode, skip):
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
        print('Data sync success!')
    else:
        printe('Data sync failure!')
        return False
    
    remote_datalist = [fs.join(loc.get_remote_metaspark_data_dir(), x) for x in datalist]
    program = '{} --internal --deploy-mode {} {}'.format(' '.join(remote_datalist), deploy_mode, '--skip' if skip else '')
    command = 'ssh {} "python3 {}/main.py deploy data {}"'.format(metacfg.ssh.ssh_key_name, loc.get_remote_metaspark_dir(), program)
    print('Connecting using key "{}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0


def _deploy_data_multiplier(multiplier, directory):
    if directory[-1] == fs.sep():
        directory = directory[:-1]
    extension = fs.basename(directory)
    num_files = int(fs.basename(fs.dirname(directory)))
    for x in range(num_files):
        source = fs.join(directory, '{}.{}'.format(x, extension))
        for y in range(multiplier-1):
            dest = fs.join(directory, '{}_{}.{}'.format(x, y, extension))
            fs.ln(source, dest, is_dir=False)
    return True


def _deploy_meta_internal(experiment):
    if experiment == None:
        try:
            experiments = exp.get_experiments()
        except RuntimeError as e:
            printe('Could not find an experiment to run. Please make an experiment in {}. See the README.md for more info.'.format(loc.get_metaspark_experiments_dir()))
            return False
    else:
        experiments = exp.load_experiment(experiment)
    for idx, x in enumerate(experiments):
        print('Starting experiment {}'.format(idx))
        if x.start():
            print('Experiment {} completed successfully'.format(idx))
        else:
            print('There were some problems during experiment {}!'.format(idx))
        x.stop()
        print('Experiment {} stopped'.format(idx))
    return True

def _deploy_meta(experiment):
    program = '--internal {}'.format(('-e '+experiment) if experiment != None else '')
    command = 'ssh {} "python3 {}/main.py deploy meta {}"'.format(metacfg.ssh.ssh_key_name, loc.get_remote_metaspark_dir(), program)
    print('Connecting using key "{}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0

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
    deployapplparser.add_argument('--flamegraph', type=str, metavar='time', help='If a time is set here (e.g. 30s, 2m, 1h), measures execution of Spark using a FlightRecording for that time')

    deployapplparser.add_argument('--internal', help=argparse.SUPPRESS, action='store_true')

    deploydataparser = subsubparsers.add_parser('data', help='Deploy data (use deploy start -h to see more...)')
    deploydataparser.add_argument('data', nargs='+', metavar='file', help='Files to place on reserved nodes local drive')
    deploydataparser.add_argument('-dm', '--deploy-mode', type=str, metavar='mode', default=str(DeployMode.STANDARD), help='Deployment mode for data', choices=[str(x) for x in DeployMode])
    deploydataparser.add_argument('--skip', help='Skip data if already found on remote', action='store_true')
    deploydataparser.add_argument('--internal', help=argparse.SUPPRESS, action='store_true')
    
    deployflameparser = subsubparsers.add_parser('flamegraph', help=argparse.SUPPRESS)
    deployflameparser.add_argument('-t', '--time', type=str, metavar='time', default='30s', help='Recording time for flamegraphs, default 30s. Pick s for seconds, m for minutes, h for hours')
    deployflameparser.add_argument('-o', '--outputdir', type=str, metavar='path', help='Record output location. Files will be stored in given absolute directorypath visible after measuring is complete')

    deploymetaparser = subsubparsers.add_parser('meta', help='Deploy applications with all variations of given parameters')
    deploymetaparser.add_argument('-e', '--experiment', type=str, metavar='experiment', help='Experiment to pick')
    deploymetaparser.add_argument('--internal', help=argparse.SUPPRESS, action='store_true')

    deploymultiplierparser = subsubparsers.add_parser('multiplier', help=argparse.SUPPRESS)
    deploymultiplierparser.add_argument('-n', '--number', type=int, metavar='amount', default='10', help='Amount of items to end with after symlinking (1 original item + x symlinks) = this number')
    deploymultiplierparser.add_argument('-d', '--dir', type=str, metavar='path', help='Dir to perform file multiplication')

    return deployparser, deployapplparser, deploydataparser, deployflameparser, deploymetaparser, deploymultiplierparser


# Return True if we found arguments used from this subparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function 
def deploy_args_set(args):
    return args.command == 'deploy'


# Processing of deploy commandline args occurs here
def deploy(parsers, args):
    deployparser, deployapplparser, deploydataparser, deployflameparser, deploymetaparser, deploymultiplierparser = parsers
    if args.subcommand == 'application':
        jarfile = args.jarfile
        mainclass = args.mainclass
        jargs = ' '.join(args.args) if args.args != None else ''
        extra_jars = ' '.join(args.jars) if args.jars != None else ''
        submit_opts = ' '.join(args.opts) if args.opts != None else ''
        if args.internal:
            return _deploy_application_internal(jarfile, mainclass, jargs, extra_jars, submit_opts, args.no_resultdir, args.flamegraph!=None, args.flamegraph)
        else:
            return _deploy_application(jarfile, mainclass, jargs, extra_jars, submit_opts, args.no_resultdir, args.flamegraph!=None, args.flamegraph)
    elif args.subcommand == 'data':
        if args.internal:
            return _deploy_data_internal(args.data, args.deploy_mode, args.skip)
        else:
            return _deploy_data(args.data, args.deploy_mode, args.skip)
    elif args.subcommand == 'flamegraph':
        return _flamegraph(args.time, args.outputdir)
    elif args.subcommand == 'meta':
        if args.internal:
            _deploy_meta_internal(args.experiment)
        else:
            _deploy_meta(args.experiment)
    elif args.subcommand == 'multiplier':
        _deploy_data_multiplier(args.number, args.dir)
    else:
        deployparser.print_help()
    return True