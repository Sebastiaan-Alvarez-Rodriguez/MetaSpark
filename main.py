#!/usr/bin/python
# The main file of MetaSpark.
# This file handles main argument parsing, 
# initial command processing and command redirection

import argparse
import os
import sys
import time

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(sys.argv[0])), 'src'))
from config.meta import cfg_meta_instance as metacfg
import config.cluster as clr
import remote.remote as rmt
from remote.reservation import Reserver
import supplier.spark as spk
import supplier.java as jv
from util.executor import Executor
import util.location as loc
import util.fs as fs
import util.time as tm
import util.ui as ui
from util.printer import *


# Check if required tools (Java11, Scala12) are available
def check(silent=False):
    a = spk.spark_available()
    b = jv.check_version(minVersion=11, maxVersion=11)
    if a and b:
        if not silent:
            prints('Requirements satisfied')
        return True
    if not silent:
        printe('Requirements not satisfied: ')
        if not a:
            print('\tSpark')
        if not b:
            print('\tJava 11')
        print()
    return False

# Handles clean commandline argument
def clean():
    return True

# Redirects server node control to dedicated code
def _exec_internal(config_filename, debug_mode):
    return rmt.run(config_filename, debug_mode)

# Handles execution on the remote main node, before booting the cluster
def exec(time_to_reserve, config_filename, debug_mode):
    print('Connected! Using cluster configuration "{}"'.format(config_filename))
    cluster_cfg = clr.load_cluster_config(config_filename)

#     time_to_reserve = time if time != '' else ui.ask_time('''
# How much time to reserve for Spark cluster with {} nodes?
# Note: Prefer reserving more time over the reservation system
# closing the cluster in the middle of your experiment.
# '''.format(cluster_cfg.nodes))
    print('Booting cluster ({} nodes) for time: {}'.format(cluster_cfg.nodes, time_to_reserve))

    # Build commands to boot the cluster
    affinity = cluster_cfg.coallocation_affinity
    nodes = cluster_cfg.nodes
    # 'echo $RESERVATION > reservation_no'
    # preserve -1  -np $NODES -t 2:00:00  | grep "Reservation number" | awk '{ print $3 }' | sed 's/://'`
    # 'prun -np {} -{} -t {} python3 {} --exec_internal {} {}'.format(nodes, affinity, time_to_reserve, fs.join(fs.abspath(), 'main.py'), config_filename, '-d' if debug_mode else '')
    
    reserver = Reserver()
    reserver.reserve(nodes, affinity, time_to_reserve)
    
    print('Booting network...')
    # Remove old logs
    fs.rm(loc.get_spark_logs_dir(), ignore_errors=True)

    try:
        executor.run()
        status = executor.wait() == 0
    except KeyboardInterrupt as e:
        print('Keyboardinterrupt received, stopping cluster for you!')
        executor.stop()
        status = True
    # except Exception as e:
    #     printw('Unexpected error found (cleaning up):')
    #     print(e)
    #     status = executor.stop() == 0

    if status:
        printc('Cluster execution complete!', Color.PRP)
    else:
        printe('Cluster execution shutdown with errors!')
    return status

# Handles export commandline argument
def export(full_exp=False):
    print('Copying files using "{}" strategy, using key "{}"...'.format('full' if full_exp else 'fast', metacfg.ssh.ssh_key_name))
    command = 'rsync -az {} {}:{}'.format(fs.abspath(),metacfg.ssh.ssh_key_name,loc.get_remote_metaspark_parent_dir())
    if full_exp:
        command+= ' --exclude '+' --exclude '.join([
            '.git',
            '__pycache__',
            'results', 
            'graphs'])
        if not clean():
            printe('Cleaning failed')
            return False
    else:
        print('[NOTE] This means we skip dep files.')
        command+= ' --exclude '+' --exclude '.join([
            '.git',
            '__pycache__',
            'results', 
            'graphs',
            'deps'])
    if os.system(command) == 0:
        prints('Export success!')
        return True
    else:
        printe('Export failure!')
        return False    


def _init_internal():
    if (not jv.check_version()):
        print('Java not ready on remote!')
        exit(1)
    elif not spk.install():
        exit(1)
    else:
        exit(0)


# Handles init commandline argument
def init():
    print('Initializing MetaSpark...')
    if not export(full_exp=True):
        printe('Unable to export to DAS5 remote using user/ssh-key "{}"'.format(metacfg.ssh_key_name))
        return False
    if os.system('ssh {} "python3 {}/main.py --init_internal"'.format(metacfg.ssh.ssh_key_name, loc.get_remote_metaspark_dir())) == 0:
        prints('Completed MetaSpark initialization. Use "{} --remote" to start execution on the remote host'.format(sys.argv[0]))
    else:
        printe('Something went wrong with MetaSpark initialization (see above). Please fix the problems and try again!')


# Handles remote commandline argument
def remote(time_to_reserve, config_filename, debug_mode, force_exp):
    if force_exp and not export(full_exp=True):
        printe('Could not export data')
        return False

    if len(config_filename) == 0: #user did not provide config, so ask for it
        config, should_export = clr.get_cluster_config()
        config_filename = fs.basename(config.path)
        if should_export:
            export(full_exp=False) #Export potential new config
    else:
        if not fs.isfile(loc.get_metaspark_cluster_conf_dir(), config_filename):
            printe('Provided config "{}" does not exist!'.format(fs.join(loc.get_metaspark_cluster_conf_dir(), config_filename)))
            return False

    program = '--exec {} -t {}'.format(config_filename, time_to_reserve) + (' -d' if debug_mode else '')

    command = 'ssh {0} "python3 {1}/main.py {2}"'.format(
        metacfg.ssh.ssh_key_name,
        loc.get_remote_metaspark_dir(),
        program)
    print('Connecting using key "{0}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0

# Redirects execution to settings.py, where user can change settings
def settings():
    metacfg.change_settings()


# The main function of MetaSpark
def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--check', help='check whether environment has correct tools', action='store_true')
    group.add_argument('--exec_internal', nargs=1, metavar='cluster_config', type=str, help=argparse.SUPPRESS)
    group.add_argument('--exec', nargs=1, metavar='cluster_config', type=str, help='call this on the DAS5 to handle server orchestration')
    group.add_argument('--export', help='export only metaspark and script code to the DAS5', action='store_true')
    group.add_argument('--init_internal', help=argparse.SUPPRESS, action='store_true')
    group.add_argument('--init', help='Initialize MetaSpark to run code on the DAS5', action='store_true')
    group.add_argument('--remote', nargs='?', metavar='cluster_config', const='.', default='.', type=str, help='execute code on the DAS5 from your local machine')
    group.add_argument('--settings', help='Change settings', action='store_true')
    parser.add_argument('-d', '--debug-mode', dest='debug_mode', help='Run remote in debug mode', action='store_true')
    parser.add_argument('-e', '--force-export', dest='force_exp', help='Forces to re-do the export phase', action='store_true')
    parser.add_argument('-t', '--time', dest='time_alloc', nargs='?', metavar='[[hh:]mm:]ss', const='15:00', default='15:00', type=str, help='Amount of time to allocate on clusters during a run')
    args = parser.parse_args()

    if args.check:
        check()
    elif args.exec_internal:
        _exec_internal(args.exec_internal[0], args.debug_mode)
    elif args.exec:
        exec(args.time_alloc, args.exec[0], args.debug_mode)
    elif args.export:
        export(full_exp=True)
    elif args.init_internal:
        _init_internal()
    elif args.init:
        init()
    elif args.remote:
        if args.remote == '.': args.remote = ''
        remote(args.time_alloc, args.remote, args.debug_mode, args.force_exp)
    elif args.settings:
        settings()

    if len(sys.argv) == 1:
        parser.print_help()

if __name__ == '__main__':
    main()