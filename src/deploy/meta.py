import argparse
import os

import dynamic.experiment as exp
from config.meta import cfg_meta_instance as metacfg


'''File containing meta deployment CLI'''


def deploy_meta(experiment_names):
    if experiment_names == None or len(experiment_names) == 0:
        experiments = exp.get_experiments()
        if len(experiments) == 0:
            printe('Could not find an experiment to run. Please make an experiment in {}. See the README.md for more info.'.format(loc.get_metaspark_experiments_dir()))
            return False
    else:
        experiments = []
        for name in experiment_names:
            found = exp.load_experiment(name)
            if found == None:
                return False
            experiments.append(found)

    def run_experiment(idx, amount, experiment):
        print('Starting experiment {}/{}'.format(idx+1, amount))
        if experiment.start(idx, amount):
            print('Experiment {}/{} completed successfully'.format(idx+1, amount))
        else:
            print('There were some problems during experiment {}!'.format(idx+1))
        experiment.stop()
        print('Experiment {} stopped'.format(idx+1))

    for idx, x in enumerate(experiments):
        run_experiment(idx, len(experiments), x)
    return True


# Deploy experiments, which can do all sorts of things which users would normally have to do manually
def deploy_meta_remote(experiments):
    program = '{}'.format(('-e '+ ' '.join(experiments)) if len(experiments) > 0 else '')
    command = 'ssh {} "python3 {}/main.py deploy meta {}"'.format(metacfg.ssh.ssh_key_name, loc.get_remote_metaspark_dir(), program)
    print('Connecting using key "{}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0


# Register 'deploy' subparser modules
def subparser(subsubparsers):
    deploymetaparser = subsubparsers.add_parser('meta', help='Deploy applications using experiments.')
    deploymetaparser.add_argument('-e', '--experiment', nargs='+', metavar='experiments', help='Experiments to deploy.')
    deploymetaparser.add_argument('--remote', help='Indicates we are not currently on DAS. Deploy experiments on remote over SSH.', action='store_true')
    return deploymetaparser


# Return True if we found arguments used from this subsubparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function
def deploy_args_set(args):
    return args.subcommand == 'meta'


def deploy(parsers, args):
    deploymetaparser = parsers
    if args.remote:
        return deploy_meta_remote(args.experiment)
    else:
        return deploy_meta(args.experiment)