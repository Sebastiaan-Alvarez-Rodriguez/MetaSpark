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
import deploy.deploy as deploy
import remote.remote as rmt
from remote.reservation import Reserver
import supplier.spark as spk
import supplier.java as jv
from util.executor import Executor
import util.location as loc
import util.fs as fs
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


# Handles execution on the remote main node, before booting the cluster
def exec(time_to_reserve, config_filename, debug_mode):
    print('Connected! Using cluster configuration "{}"'.format(config_filename))
    cluster_cfg = clr.load_cluster_config(config_filename)


    nodes = cluster_cfg.nodes + 1 # We always want 1 node for the spark master alone
    if cluster_cfg.coallocation_affinity > 1:
        nodes = cluster_cfg.coallocation_affinity * cluster_cfg.nodes+1
        printw('''
Warning! Configuration specifies "{}" workers per node, {} nodes.
MetaSpark version RESERVE can only spawn 1 process per node,
due to some annoying things in Spark 3.0+.
Spawning {} nodes instead to service your request!
'''.format(cluster_cfg.coallocation_affinity, cluster_cfg.nodes, nodes-1))

    try:
        reserver = Reserver.load()
        if ui.ask_bool('Found active reservation. Kill it and spawn new (Y) or cancel (n)?'):
            if reserver.stop():
                prints('Stopped reservation successfully!')
            else:
                return False
    except Exception as e:
        pass
    print('Booting cluster ({} nodes) for time: {}'.format(cluster_cfg.nodes, time_to_reserve))

    reserver = Reserver()
    reserver.reserve(nodes, time_to_reserve)
    
    # Remove old logs
    fs.rm(loc.get_spark_logs_dir(), ignore_errors=True)

    print('Booting network...')
    
    # 'echo $RESERVATION > reservation_no'
    # preserve -1  -np $NODES -t 2:00:00  | grep "Reservation number" | awk '{ print $3 }' | sed 's/://'`
    # 'prun -np {} -{} -t {} python3 {} --exec_internal {} {}'.format(nodes, affinity, time_to_reserve, fs.join(fs.abspath(), 'main.py'), config_filename, '-d' if debug_mode else '')

    # Boot master first
    status = rmt.boot_master(reserver.deployment.nodes[0], debug_mode=debug_mode)
    if not status:
        return False

    time.sleep(5) #Give master deamon a head start

    # Boot all slaves in parallel
    status = rmt.boot_slaves(reserver.deployment.nodes[1:], reserver.deployment.nodes[0], debug_mode=debug_mode)
    
    # Persists reservation info (reservation number, nodes)
    reserver.persist()
    
    if status:
        printc('Cluster deployment complete!', Color.PRP)
    else:
        printe('Cluster deployment stopped with errors!')
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
def remote_start(time_to_reserve, config_filename, debug_mode, force_exp):
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

# Stop the remote cluster, if any is running
def remote_stop():
    program = '--stop'
    command = 'ssh {0} "python3 {1}/main.py {2}"'.format(
        metacfg.ssh.ssh_key_name,
        loc.get_remote_metaspark_dir(),
        program)
    print('Connecting using key "{0}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0


# Redirects execution to settings.py, where user can change settings
def settings():
    metacfg.change_settings()

# Stop cluster running here, if any is running
def stop(silent=False):
    try:
        reserver = Reserver.load()
        retval = reserver.stop()
        if not silent:
            if retval:
                prints('Reservation {} successfully stopped!'.format(reserver.reservation_number))
            else:
                printe('Could not stop reservation with id {}'.format(reserver.reservation_number))
        return retval
    except FileNotFoundError as e:
        if not silent: print('No reservation found on remote')
    except Exception as e:
        if not silent: print('Reservation file found, no longer active')

# The main function of MetaSpark
def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parser.add_subparsers(help='Subcommands', dest='command')
    deploy.subparser(subparsers)

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--check', help='check whether environment has correct tools', action='store_true')
    group.add_argument('--exec_internal', nargs=1, metavar='cluster_config', type=str, help=argparse.SUPPRESS)
    group.add_argument('--exec', nargs=1, metavar='cluster_config', type=str, help='call this on the DAS5 to handle server orchestration')
    group.add_argument('--export', help='export only metaspark and script code to the DAS5', action='store_true')
    group.add_argument('--init_internal', help=argparse.SUPPRESS, action='store_true')
    group.add_argument('--init', help='Initialize MetaSpark to run code on the DAS5', action='store_true')
    group.add_argument('--remote-start', dest='remote_start', nargs='?', metavar='cluster_config', type=str, help='execute code on the DAS5 from your local machine')
    group.add_argument('--remote-stop', dest='remote_stop', help='Stop cluster on DAS5 from your local machine', action='store_true')
    group.add_argument('--settings', help='Change settings', action='store_true')
    group.add_argument('--stop', help='Stop any cluster that currently runs on this machine', action='store_true')
    parser.add_argument('-d', '--debug-mode', dest='debug_mode', help='Run remote in debug mode', action='store_true')
    parser.add_argument('-e', '--force-export', dest='force_exp', help='Forces to re-do the export phase', action='store_true')
    parser.add_argument('-t', '--time', dest='time_alloc', nargs='?', metavar='[[hh:]mm:]ss', const='15:00', default='15:00', type=str, help='Amount of time to allocate on clusters during a run')
    args = parser.parse_args()

    if deploy.deploy_args_set(args):
        return deploy.deploy(parser, args)
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
    elif args.remote_start:
        remote_start(args.time_alloc, args.remote_start, args.debug_mode, args.force_exp)
    elif args.remote_stop:
        remote_stop()
    elif args.stop:
        stop()
    elif args.settings:
        settings()

    if len(sys.argv) == 1:
        parser.print_help()

if __name__ == '__main__':
    main()