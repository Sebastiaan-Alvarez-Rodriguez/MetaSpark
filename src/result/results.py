# This file handles argument parsing for result creation

import argparse
import os
import socket

from config.meta import cfg_meta_instance as metacfg
from remote.reservation import Reserver
import remote.util.ip as ip
from util.executor import Executor
import util.fs as fs
import util.importer as importer
import util.location as loc
from util.printer import *
import util.time as tm
import util.ui as ui


# Register 'deploy' subparser modules
def subparser(subparsers):
    resultparser = subparsers.add_parser('results', help='Create result graphs/statistics')
    subsubparsers = resultparser.add_subparsers(help='Subsubcommands', dest='subcommand')
    filterparser = subsubparsers.add_parser('filter', help='Display generic info, based on filters')
    subsubsubparsers = filterparser.add_subparsers(help='Subsubsubcommands', dest='subsubcommand')
    filtergenericparser = subsubparsers.add_parser('generic', help='Print generic info, using filters')
    filternormalparser = subsubparsers.add_parser('normal', help='Print normal info, using filters')

    filterparser.add_argument('-p', '--partition', nargs='+', metavar='filter', help='Partition filters')
    filterparser.add_argument('-e', '--extension', nargs='+', metavar='filter', help='Extension filters')
    filterparser.add_argument('-a', '--amount', nargs='+', metavar='filter', help='Amount filters')
    filterparser.add_argument('-k', '--kind', nargs='+', metavar='filter', help='Kind filters')
    filterparser.add_argument('-rb', '--readbuffer', nargs='+', metavar='filter', help='Reeadbuffer filters')

    resultparser.add_argument('data', help='Location of data!', type=str)
    resultparser.add_argument('-l', '--large', help='Forces to generate large graphs, with large text', action='store_true')
    resultparser.add_argument('-ns', '--no-show', dest='no_show', help='Do not show generated graph (useful on servers without xorg forwarding)', action='store_true')
    resultparser.add_argument('-s', '--store', help='Store generated graph (in /metazoo/graphs/<graph_name>/<timestamp>.<type>)', action='store_true')
    resultparser.add_argument('-t', '--type', nargs=1, help='Preferred storage type (default=pdf)', default='pdf')
    return resultparser

# Return True if we found arguments used from this subparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function 
def results_args_set(args):
    return args.command == 'results'

# Processing of result commandline args occurs here
def results(parser, args):
    # We explicitly MUST check if matplotlib is available to import
    # If it is not, we cannot process results on the current machine
    if not importer.library_exists('matplotlib'):
        printe('Cannot work with results. Matplotlib is not available!')
        return

    if not importer.library_exists('numpy'):
        printe('Cannot work with results. Numpy is not available!')
        return

    if args.store and args.type is None:
        parser.error('--store (-st) requires --type (-t)')
        return
    import result.util.storer as storer # We can only import storer here, as it depends on matplotlib and we don't want to check matplotlib availibility again
    if args.store and not storer.filetype_is_supported(args.type):
        parser.error('--type only supports filetypes: '+', '.join(storer.supported_filetypes()))
        return

    if not fs.isdir(loc.get_metaspark_results_dir()):
        printe('[FAILURE] You have no experiment results directory "{}". Run experiments to get some data first.'.format(log.get_metaspark_results_dir()))
    fargs = [args.data, args.large, args.no_show, args.store, args.type]

    if args.subcommand == 'filter':
        import result.filter.generic as f
        f.stats(args.data, args.partition, args.extension, args.amount, args.kind, args.readbuffer)
    else:
        parser.print_help()