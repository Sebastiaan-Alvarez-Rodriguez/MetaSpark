import argparse
import os
import re
import subprocess

from util.printer import *


'''File containing flamegraph deployment CLI'''


# Finds a java process with enabled flightrecorder matching regex
# Returns integer PID if found, None otherwise
def _util_find_proc_regex(regex='([0-9]+) .*ExecutorBackend'):
    out = subprocess.check_output('$JAVA_HOME/bin/jps', shell=True).decode('utf-8').strip()
    res = re.search(regex, out)

    # if res != None:
    #     import socket
    #     ans = int(res.group(1))
    #     print('{} found that the following contains a good entry:\n{}\nAnswer: {}'.format(socket.gethostname(), out, ans))
    return int(res.group(1)) if res != None else None


# Launch flightrecording for given pid, for specified duration, with specified delay before recording.
# Default delay is 0 seconds (s). Can also specify minutes (m) or hours (h).
# Default duration is in seconds (s). Can also specify minutes (m) or hours (h).
# Returns directly after starting the flightrecording.
# Outputs a .jfr file on given absolute path, after specified delay+duration has passed.
def _util_launch_flightrecord(pid, abs_path, duration='30s', delay='0s'):
    command = '$JAVA_HOME/bin/jcmd {} JFR.start'.format(pid)
    command += ' delay={}'.format(delay) if delay != '0s' else ''
    command += ' duration={} filename={} settings=profile'.format(duration, abs_path)
    os.system(command)


# Flamegraph function executed on node-level. Starts the reading process
def deploy_flamegraph_node(is_master, gid, flame_graph_duration, base_recordpath):
    designation = 'driver' if is_master else 'worker'
    recordpath = fs.join(base_recordpath, designation+str(gid)+'.jfr')
    pid = None
    tries = 100
    while pid == None and tries > 0:
        pid = _util_find_proc_regex()
        tries -= 1
    if pid == None:
        import socket
        printw('{}: Unable to find jPID, skipping flamegraph'.format(socket.gethostname()))
        return False
    else:
        _util_launch_flightrecord(pid, recordpath, duration=flame_graph_duration)
        printc('Flight recording started, output will be at {}'.format(recordpath), Color.CAN)
    return True


# Coordinates flamegraph listening deployment. Boots flamegraph deployment on all nodes
def deploy_flamegraph(reservation_or_number, flame_graph_duration, only_master=False, only_worker=False):
    from remote.reserver import reservation_manager
    try:
        reservation = reservation_manager.get(reservation_or_number) if isinstance(reservation_or_number, int) else reservation_or_number
        if not reservation.validate():
            printe('Reservation no longer valid. Cannot make flamegraphs')
            return False
    except FileNotFoundError as e:
        printe('No reservation found on remote. Cannot run!')
        return False
    except Exception as e:
        printe('Reservation file found, no longer active')
        print(e)
        return False

    executors = []
    base_recordpath = fs.join(loc.get_metaspark_recordings_dir(), tm.timestamp('%Y-%m-%d_%H:%M:%S.%f'))
    fs.mkdir(base_recordpath, exist_ok=False)
    for host in reservation.deployment.nodes:
        flame_command = 'ssh {} "python3 {}/main.py deploy flamegraph_node {} -t {} -o {}"'.format(host, fs.abspath(), reservation.deployment.get_gid(host), flame_graph_duration, base_recordpath)
        if reservation.deployment.is_master(host):
            if only_worker:
                continue # We skip master
            flame_command += ' -m'
        else:
            if only_master:
                continue # We skip workers
        executors.append(Executor(flame_command, shell=True))

    Executor.run_all(executors) # Connect to all nodes, start listening for correct pids
    print('Flamegraph reading set for {}. listening started...'.format(flame_graph_duration))
    return Executor.wait_all(executors, stop_on_error=False)


# Register 'deploy' subparser modules
def subparser(subsubparsers):
    deployflamenodeparser = subsubparsers.add_parser('flamegraph_node', help=argparse.SUPPRESS)
    deployflamenodeparser.add_argument('gid', type=int, help='Global id of this node')
    deployflamenodeparser.add_argument('-m', '--is-master', dest='is_master', help='Whether this node is the driver or not', action='store_true')
    deployflamenodeparser.add_argument('-t', '--time', type=str, metavar='time', default='30s', help='Recording time for flamegraphs, default 30s. Pick s for seconds, m for minutes, h for hours')
    deployflamenodeparser.add_argument('-o', '--outputdir', type=str, metavar='path', help='Record output location. Files will be stored in given absolute directorypath visible after measuring is complete')

    deployflameparser = subsubparsers.add_parser('flamegraph', help='Manually make flamegraph recordings')
    deployflameparser.add_argument('reservation_number', type=int, help='Number of reservation to use')
    deployflameparser.add_argument('-t', '--time', type=str, metavar='time', default='30s', help='Recording time for flamegraphs, default 30s. Pick s for seconds, m for minutes, h for hours')
    deployflameparser.add_argument('-om', '--only-master', dest='only_master', help='Whether we will make a flamegraph for only the master nodes or not', action='store_true')
    deployflameparser.add_argument('-ow', '--only-worker', dest='only_worker', help='Whether we will make a flamegraph for only the worker nodes or not', action='store_true')
    return deployflamenodeparser, deployflameparser


# Return True if we found arguments used from this subsubparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function
def deploy_args_set(args):
    return args.subcommand == 'flamegraph' or args.subcommand == 'flamegraph_node'


def deploy(parsers, args):
    deployflamenodeparser, deployflameparser = parsers
    if args.subcommand == 'flamegraph_node':
        return deploy_flamegraph_node(args.is_master, args.gid, args.time, args.outputdir)
    if args.subcommand == 'flamegraph':
        return deploy_flamegraph(args.reservation_number, args.time, only_master=args.only_master, only_worker=args.only_worker)