import argparse
import os

import config.ssh as ssh
from deploy.allocator.allocator import Allocator
import dynamic.experiment as exp
import util.fs as fs
import util.location as loc
from util.printer import *


'''File containing allocated deployment CLI'''


# Start a meta deployment on this cluster, immediately releasing NO_HANGUP triggers and immediately returning.
def deploy_allocated_cluster(experiment_name):
    fs.mkdir(loc.get_metaspark_logs_dir(), exist_ok=True)
    return os.command('nohup python3 {}/main.py deploy meta -e {} > {}.log &'.format(fs.abspath(), experiment_name, fs.join(loc.get_metaspark_logs_dir(), experiment_name)))


# Execute experiments on multiple clusters, one at a time per cluster
def deploy_allocated(experiment_names):
    if experiment_names == None or len(experiment_names) == 0:
        experiments = exp.get_experiments()
        if len(experiments) == 0:
            printe('Could not find an experiment to run. Please make an experiment in {}. See the README.md for more info.'.format(loc.get_metaspark_experiments_dir()))
            return False
    else:
        experiments = []
        for name in experiment_names:
            experiments.append(exp.load_experiment(name))

    experiments_sorted = sorted(experiments, key=lambda x: x.max_nodes_needed(), reverse=True)

    def allocator_func(cluster, experiment):
        name = fs.basename(experiment.location)
        name = name[:-3] if name.endswith('.py') else name
        print('Allocating experiment "{}" on cluster "{}"'.format(name, cluster.ssh_key_name))

        program = 'python3 {}/main.py deploy meta --no-hup -e {}'.format(loc.get_remote_metaspark_dir(cluster), name)
        command = 'ssh {0} "mkdir -p {1} && {2} > {1}/{3}.log"'.format(cluster.ssh_key_name, loc.get_remote_metaspark_logs_dir(), program, name)
        print('Connecting using key "{}"...'.format(cluster.ssh_key_name))
        if os.system(command) != 0:
            printw('Got a non-zero return code for allocating experiment {} on cluster {}'.format(fs.basename(experiment), cluster.ssh_key_name))

    clusters = ssh.get_configs()
    alloc = Allocator(experiments, clusters, allocator_func)
    alloc.execute()


# Register 'deploy' subparser modules
def subparser(subsubparsers):
    deployallocatedparser = subsubparsers.add_parser('allocated', help='Deploy applications using experiments across the entire DAS system.')
    deployallocatedparser.add_argument('-e', '--experiment', nargs='+', metavar='experiments', help='Experiments to deploy.')   
    deployallocatedparser.add_argument('-c', '--cluster', help=argparse.SUPPRESS, action='store_true')   
    return deployallocatedparser


# Return True if we found arguments used from this subsubparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function
def deploy_args_set(args):
    return args.subcommand == 'allocated'


def deploy(parsers, args):
    deployallocatedparser = parsers
    if args.cluster:
        return deploy_allocated_cluster(args.experiment)
    else:
        return deploy_allocated(args.experiment)