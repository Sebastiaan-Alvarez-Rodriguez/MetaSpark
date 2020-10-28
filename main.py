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
import result.results as res
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
def _exec_internal(debug_mode=False):
    return rmt.run(debug_mode)

# Handles execution on the remote main node, before booting the cluster
def exec(time, exec_time=None, debug_mode=False):
    print('Connected!')
    print('GIVEN TIME: '+time)
    cluster_cfg = clr.get_cluster_config()

    time_to_reserve = exec_time if exec_time != None else ui.ask_time('''
How much time to reserve for Spark cluster with {} nodes?
Note: Prefer reserving more time over a closing cluster
in the middle of your experiment.
''')


    fs.rm(loc.get_cfg_dir(), '.metaspark.cfg', ignore_errors=True)
    fs.rm(loc.get_metaspark_experiment_dir(), '.port.txt', ignore_errors=True)
        
    # Build commands to boot the experiment
    affinity = config.coallocation_affinity
    nodes = config.nodes
    command = 'prun -np {} -{} -t {} python3 {} --exec_internal {}'.format(nodes, affinity, time_to_reserve, fs.join(fs.abspath(), 'main.py'), '-d' if debug_mode else '')
    
    print('Booting network...')
    executor = Executor(command)

    executor.run(shell=True)
    status = executor.wait() == 0

    # TODO: Cleanup

    if status:
        printc('Experiment {}/{} complete!'.format(idx+1, len(experiments)), Color.PRP)
    else:
        printe('Experiment {}/{} had errors!'.format(idx+1, len(experiments)))
    return status

# Handles export commandline argument
def export(full_exp=False):
    print('Copying files using "{}" strategy, using key "{}"...'.format('full' if full_exp else 'fast', metacfg.ssh.ssh_key_name))
    if full_exp:
        command = 'rsync -az {} {}:{} {} {} {} {}'.format(
            fs.abspath(),
            metacfg.ssh.ssh_key_name,
            loc.get_remote_metaspark_parent_dir(),
            '--exclude .git',
            '--exclude __pycache__',
            '--exclude results', 
            '--exclude graphs')
        if not clean():
            printe('Cleaning failed')
            return False
    else:
        print('[NOTE] This means we skip dep files.')
        command = 'rsync -az {} {}:{} {} {} {} {} {}'.format(
            fs.dirname(fs.abspath()),
            metacfg.ssh.ssh_key_name,
            loc.get_remote_parent_dir(),
            '--exclude .git',
            '--exclude __pycache__',
            '--exclude results',
            '--exclude graphs',
            '--exclude deps')
    if os.system(command) == 0:
        prints('Export success!')
        return True
    else:
        printe('Export failure!')
        return False    


# Handles init commandline argument
def init():
    print('Initializing MetaSpark...')
    if not export(full_exp=True):
        printe('Unable to export to DAS5 remote using user/ssh-key "{}"'.format(metacfg.ssh_key_name))
        return False
    prints('Completed MetaSpark initialization. Use "{} --remote" to start execution on the remote host'.format(sys.argv[0]))

# Handles remote commandline argument
def remote(time, force_exp=False, debug_mode=False):
    if force_exp and not export(full_exp=True):
        printe('Could not export data')
        return False

    program = '--exec {}'.format(time) + (' -d' if debug_mode else '')

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
    subparser = parser.add_subparsers()
    res.subparser(subparser)

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--check', help='check whether environment has correct tools', action='store_true')
    group.add_argument('--exec_internal', help=argparse.SUPPRESS, action='store_true')
    group.add_argument('--exec', nargs='?', metavar='[[hh:]mm:]ss', help='call this on the DAS5 to handle server orchestration')
    group.add_argument('--export', help='export only metaspark and script code to the DAS5', action='store_true')
    group.add_argument('--init', help='Initialize MetaSpark to run code on the DAS5', action='store_true')
    group.add_argument('--remote', nargs='?', metavar='[[hh:]mm:]ss', help='execute code on the DAS5 from your local machine')
    group.add_argument('--settings', help='Change settings', action='store_true')
    parser.add_argument('-c', '--force-compile', dest='force_comp', help='Forces to (re)compile Zookeeper, even when build seems OK', action='store_true')
    parser.add_argument('-d', '--debug-mode', dest='debug_mode', help='Run remote in debug mode', action='store_true')
    parser.add_argument('-e', '--force-export', dest='force_exp', help='Forces to re-do the export phase', action='store_true')
    args = parser.parse_args()

    if res.result_args_set(args):
        res.results(parser, args)
    elif args.check:
        check()
    elif args.exec_internal:
        _exec_internal(args.debug_mode)
    elif args.exec:
        exec(args.exec[0], debug_mode=args.debug_mode)
    elif args.export:
        export(full_exp=True)
    elif args.init:
        init()
    elif args.remote:
        remote(args.remote, force_exp=args.force_exp, debug_mode=args.debug_mode)
    elif args.settings:
        settings()

    if len(sys.argv) == 1:
        parser.print_help()

if __name__ == '__main__':
    main()