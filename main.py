#!/usr/bin/python
# The main file of MetaSpark.
# This file handles main argument parsing, 
# initial command processing and command redirection

import argparse
import os
import sys
import time
import threading

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(sys.argv[0])), 'src'))
from config.meta import cfg_meta_instance as metacfg
import config.cluster as clr
import deploy.deploy as deploy
import remote.remote as rmt
from remote.util.deploymode import DeployMode
import result.results as results
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


# Handles export commandline argument
def export(full_exp=False):
    print('Copying files using "{}" strategy, using key "{}"...'.format('full' if full_exp else 'fast', metacfg.ssh.ssh_key_name))
    command = 'rsync -az {} {}:{}'.format(fs.abspath(), metacfg.ssh.ssh_key_name, loc.get_remote_metaspark_parent_dir())
    if full_exp:
        command+= ' --exclude '+' --exclude '.join([
            '.git',
            '__pycache__',
            'data',
            'graphs',
            'jars',
            'logs',
            'recordings',
            'results'])
        if not clean():
            printe('Cleaning failed')
            return False
    else:
        print('[NOTE] This means we skip dep files.')
        command+= ' --exclude '+' --exclude '.join([
            '.git',
            '__pycache__',
            'deps',
            'data',
            'graphs',
            'jars',
            'logs',
            'recordings',
            'results'])
    if os.system(command) == 0:
        prints('Export success!')
        return True
    else:
        printe('Export failure!')
        return False


def _init_internal():
    if (not jv.check_version()):
        print('Java not ready on remote!')
        return False
    return spk.install()


# Handles init commandline argument
def init():
    print('Initializing MetaSpark...')
    if not export(full_exp=True):
        printe('Unable to export to DAS5 remote using user/ssh-key "{}"'.format(metacfg.ssh_key_name))
        return False
    if os.system('ssh {} "python3 {}/main.py init --internal"'.format(metacfg.ssh.ssh_key_name, loc.get_remote_metaspark_dir())) == 0:
        prints('Completed MetaSpark initialization. Use "{} remote start" to start execution on the remote host'.format(sys.argv[0]))
        return True
    else:
        printe('Something went wrong with MetaSpark initialization (see above). Please fix the problems and try again!')
    return False

# Handles remote commandline argument
def remote_start(time_to_reserve, config_filename, debug_mode, deploy_mode, force_exp):
    if force_exp and not export(full_exp=True):
        printe('Could not export data')
        return False

    config = clr.get_or_create_cluster_config(config_filename)
    if not config:
        return False

    program = 'start -c {} -t {} -dm {}'.format(fs.basename(config.path), time_to_reserve, deploy_mode)+(' -d' if debug_mode else '')

    command = 'ssh {} "python3 {}/main.py {}"'.format(
        metacfg.ssh.ssh_key_name,
        loc.get_remote_metaspark_dir(),
        program)
    print('Connecting using key "{}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0


# Stop the remote cluster, if any is running
def remote_stop():
    program = 'stop'
    command = 'ssh {} "python3 {}/main.py {}"'.format(
        metacfg.ssh.ssh_key_name,
        loc.get_remote_metaspark_dir(),
        program)
    print('Connecting using key "{}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0


# Redirects execution to settings.py, where user can change settings
def settings():
    return metacfg.change_settings()


# Starts cluster, without starting Spark.
# Returns reservation
def _start_cluster(time_to_reserve, config_filename):
    cluster_cfg = clr.get_or_create_cluster_config(config_filename)
    if not cluster_cfg:
        return False

    nodes = cluster_cfg.nodes + 1 # We always want 1 node for the spark master alone
    if cluster_cfg.coallocation_affinity > 1:
        nodes = cluster_cfg.coallocation_affinity * cluster_cfg.nodes+1
        printw('''
Warning! Configuration specifies "{}" workers per node, {} nodes.
MetaSpark version RESERVE can only spawn 1 Spark process per node,
due to some annoying things in Spark.
Spawning {} nodes instead to service your request!
'''.format(cluster_cfg.coallocation_affinity, cluster_cfg.nodes, nodes-1))

    print('Booting cluster ({} nodes) for time: {}'.format(nodes, time_to_reserve))
    from remote.reserver import reservation_manager
    reservation = reservation_manager.reserve(nodes, time_to_reserve, cluster_cfg.infiniband)
    
    # Remove old logs
    fs.rm(loc.get_spark_logs_dir(), ignore_errors=True)
    return reservation


def _start_spark_on_cluster(reservation, debug_mode, deploy_mode, spark_master_port=7077):
    print('Booting network...')
    # Boot master first
    status = rmt.boot_master(reservation.deployment.master_ip, port=spark_master_port, debug_mode=debug_mode)
    if not status:
        return False

    time.sleep(5) #Give master deamon a head start

    # Boot all slaves in parallel
    status = rmt.boot_slaves(reservation.deployment.slave_ips, reservation.deployment.master_ip, master_port=spark_master_port, debug_mode=debug_mode, deploy_mode=deploy_mode)

    reservation.deployment.master_port = spark_master_port
    # Persists reservation info (reservation number, nodes)
    from remote.reserver import reservation_manager
    reservation_manager.update_reservation(reservation)

    if status:
        printc('Cluster deployment complete!', Color.PRP)
    else:
        printe('Cluster deployment stopped with errors!')
    return status

# Handles execution on the remote main node, before booting the cluster
def start(time_to_reserve, config_filename, debug_mode, deploy_mode):
    print('Using cluster configuration "{}"'.format(config_filename))
    deploy_mode = DeployMode.interpret(deploy_mode)
    reservation = _start_cluster(time_to_reserve, config_filename)
    return _start_spark_on_cluster(reservation, debug_mode, deploy_mode, spark_master_port=7077)


# Stop cluster running here, if any is running
def stop(numbers, silent=False):
    from remote.reserver import reservation_manager
    if numbers == None or numbers == '' or numbers == []:
        return reservation_manager.stop_all()
    elif isinstance(numbers, list):
        reservation_manager.stop_selected(numbers)
    else:
        try:
            number_int = int(numbers)
        except Exception as e:
            prints('Could not convert "{}" to a number!'.format(number))
            return False
        try:
            retval = reservation_manager.stop(number_int)
            if not silent:
                if retval:
                    prints('Reservation {} successfully stopped!'.format(number_int))
                else:
                    printe('Could not stop reservation with id {}'.format(number_int))
            return retval
        except FileNotFoundError as e:
            if not silent: print('Reservation file for number {} found'.format(number_int))
        except Exception as e:
            if not silent: print('Reservation file found, no longer active')
    return True


# Process parsed commandline arguments. Returns whether command was successful
def cli_process(args, parser, deployparsers, resultparser):
    if deploy.deploy_args_set(args):
        retval = deploy.deploy(deployparsers, args)
    elif results.results_args_set(args):
        retval = results.results(resultparser, args)
    elif args.command == 'check':
        retval = check()
    elif args.command == 'export':
        retval = export(full_exp=False)
    elif args.command == 'init' and args.internal:
        retval = _init_internal()
    elif args.command == 'init':
        retval = init()
    elif args.command == 'remote':
        if args.subcommand=='start':
            retval = remote_start(args.time_alloc, args.clusterconfig, args.debug_mode, args.deploy_mode, args.force_exp)
        elif args.subcommand=='stop':
            retval = remote_stop()
        else:
            remoteparser.print_help()
            retval = 1
    elif args.command == 'start':
        retval = start(args.time_alloc, args.clusterconfig, args.debug_mode, args.deploy_mode)
    elif args.command == 'stop':
        retval = retval = stop(args.number)
    elif args.command == 'settings':
        retval = settings()
    else:
        parser.print_help()
        exit(1)
    return retval

# The main function of MetaSpark
def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--sshconfig', metavar='config', type=str, help='Optionally override SSH config filename to use for execution')

    subparsers = parser.add_subparsers(help='Subcommands', dest='command')
    deployparsers = deploy.subparser(subparsers)
    resultparser = results.subparser(subparsers)
    checkparser = subparsers.add_parser('check', help='check whether environment has correct tools')
    
    startparser = subparsers.add_parser('start', help='call this on the DAS5 to handle server orchestration')
    startparser.add_argument('-c', '--clusterconfig', metavar='config', type=str, help='Cluster config filename to use for execution')
    startparser.add_argument('-d', '--debug-mode', dest='debug_mode', help='Run remote in debug mode', action='store_true')
    startparser.add_argument('-dm', '--deploy-mode', dest='deploy_mode', type=str, metavar='mode', default=str(DeployMode.STANDARD), help='Deployment mode for cluster', choices=[str(x) for x in DeployMode])
    startparser.add_argument('-t', '--time', dest='time_alloc', nargs='?', metavar='[[hh:]mm:]ss', const='15:00', default='15:00', type=str, help='Amount of time to allocate on clusters during a run')
    startparser.add_argument('--internal', nargs=1, type=str, help=argparse.SUPPRESS)

    exportparser = subparsers.add_parser('export', help='export only metaspark and script code to the DAS5')
    
    initparser = subparsers.add_parser('init', help='Initialize MetaSpark to run code on the DAS5')
    initparser.add_argument('--internal', help=argparse.SUPPRESS, action='store_true')

    remoteparser = subparsers.add_parser('remote', help='Start or stop a cluster running on the remote')
    subsubparsers = remoteparser.add_subparsers(help='Subsubcommands', dest='subcommand')
    remotestartparser = subsubparsers.add_parser('start', help='Start cluster on DAS5 from your local machine')
    remotestartparser.add_argument('-c', '--clusterconfig', metavar='config', type=str, help='Cluster config filename to use for execution')
    remotestartparser.add_argument('-d', '--debug-mode', dest='debug_mode', help='Run remote in debug mode', action='store_true')
    remotestartparser.add_argument('-dm', '--deploy-mode', dest='deploy_mode', type=str, metavar='mode', default=str(DeployMode.STANDARD), help='Deployment mode for cluster', choices=[str(x) for x in DeployMode])
    remotestartparser.add_argument('-e', '--force-export', dest='force_exp', help='Forces to re-do the export phase', action='store_true')
    remotestartparser.add_argument('-t', '--time', dest='time_alloc', nargs='?', metavar='[[hh:]mm:]ss', const='15:00', default='15:00', type=str, help='Amount of time to allocate on clusters during a run')

    remotestopparser = subsubparsers.add_parser('stop', help='Stop cluster on DAS5 from your local machine')

    settingsparser = subparsers.add_parser('settings', help='Change settings')
    stopparser = subparsers.add_parser('stop',  help='Stop a given reservation or all reservations that currently running on this machine')
    stopparser.add_argument('-n', '--number', nargs='*', metavar='number', type=int, help='Reservation number to stop. If none given, stops all known reservations.')
    
    args = parser.parse_args()


    if args.sshconfig:
        if args.sshconfig == '*' or args.sshconfig == '+':
            from config.meta import get_metaspark_ssh_conf_dir
            retval = True
            if args.sshconfig == '*':
                for x in fs.ls(get_metaspark_ssh_conf_dir()):
                    print('Processing for SSH config: {}'.format(x))
                    metacfg.reload_ssh(x)
                    retval &= cli_process(args, parser, deployparsers, resultparser)
        else: 
            metacfg.reload_ssh(args.sshconfig)
            retval = cli_process(args, parser, deployparsers, resultparser)
    else:
        retval = cli_process(args, parser, deployparsers, resultparser)

    if isinstance(retval, bool):
        exit(0 if retval else 1)
    else: #retval will be an int
        exit(retval)

if __name__ == '__main__':
    main()