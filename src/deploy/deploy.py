# This file handles argument parsing for deployments,
# as well as actual deployment.

import argparse
import glob
import os
import socket
import time

import config.ssh as ssh
from config.meta import cfg_meta_instance as metacfg

from deploy.allocator.allocator import Allocator
import deploy.flamegraph as fg
import dynamic.experiment as exp
from remote.util.deploymode import DeployMode
import remote.util.ip as ip
from util.executor import Executor
import util.fs as fs
import util.location as loc
from util.printer import *


'''File containing deployment CLI'''


def _get_modules():
    import deploy.allocated as allocated
    import deploy.application as application
    import deploy.data as data
    import deploy.flamegraph as flamegraph
    import deploy.meta as meta
    import deploy.util as deployutil
    return [allocated, application, data, flamegraph, meta, deployutil]


# Register 'deploy' subparser modules
def subparser(subparsers):
    deployparser = subparsers.add_parser('deploy', help='Deploy applications/data (use deploy -h to see more...)')
    subsubparsers = deployparser.add_subparsers(help='Subsubcommands', dest='subcommand')
    return [x.subparser(subsubparsers) for x in _get_modules()]


# Return True if we found arguments used from this subparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function 
def deploy_args_set(args):
    return args.command == 'deploy'


# Processing of deploy commandline args occurs here
def deploy(parsers, args):
    for parsers_for_module, module in zip(parsers, _get_modules()):
        if module.deploy_args_set(args):
            return module.deploy(parsers_for_module, args)
    deployparser.print_help()
    return True