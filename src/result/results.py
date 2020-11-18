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

def filename_to_rb(filename):
    return int(filename.split('.')[-2])

# Merge continuation files (e.g. x.res_0 is cont. file for x.res)
def merge(data):
    def file_append(source, target):
        with open(source, 'r') as s:
            with open(target, 'a') as t:
                while True:
                    data = s.read(65536)
                    if data:
                        t.write(data)
                    else:
                        break

    datapath = fs.join(loc.get_metaspark_results_dir(), data)
    merges = 0
    for dpartition in sorted(fs.ls(datapath, only_dirs=True), key=lambda x: int(x)):
        for dextension in fs.ls(fs.join(datapath, dpartition), only_dirs=True):
            for damount in sorted(fs.ls(fs.join(datapath, dpartition, dextension), only_dirs=True), key=lambda x: int(x)):
                for dkind in fs.ls(fs.join(datapath, dpartition, dextension, damount), only_dirs=True):
                    flist = sorted([x for x in fs.ls(fs.join(datapath, dpartition, dextension, damount, dkind), only_files=True, full_paths=True) if x.split('.')[-1].startswith('res')], key=lambda x: filename_to_rb(x)+len(x))
                    if len(flist) == 0:
                        continue
                    fbase = flist[0]
                    for file in flist:
                        if not file.endswith('.res'):
                            print('Move data from {} to {}'.format(fs.basename(file), fs.basename(fbase)))
                            file_append(file, fbase)
                            fs.rm(file)
                            merges += 1
                        else:
                            fbase = file
    prints('Merged {} files {}'.format(merges, '(all clean)' if merges == 0 else ''))


# Register 'deploy' subparser modules
def subparser(subparsers):
    resultparser = subparsers.add_parser('results', help='Create result graphs/statistics')
    subsubparsers = resultparser.add_subparsers(help='Subsubcommands', dest='subcommand')
    filterparser = subsubparsers.add_parser('filter', help='Display generic info, based on filters')
    mergeparser = subsubparsers.add_parser('merge', help='Merge continuation files into main result files')
    
    # subsubsubparsers = filterparser.add_subparsers(help='Subsubsubcommands', dest='subsubcommand')
    # filtergenericparser = subsubparsers.add_parser('generic', help='Print generic info, using filters')
    # filternormalparser = subsubparsers.add_parser('normal', help='Print normal info, using filters')

    filterparser.add_argument('-p', '--partition', nargs='+', metavar='filter', help='Partition filters')
    filterparser.add_argument('-e', '--extension', nargs='+', metavar='filter', help='Extension filters')
    filterparser.add_argument('-a', '--amount', nargs='+', metavar='filter', help='Amount filters')
    filterparser.add_argument('-k', '--kind', nargs='+', metavar='filter', help='Kind filters')
    filterparser.add_argument('-rb', '--readbuffer', nargs='+', metavar='filter', help='Readbuffer filters')
    filterparser.add_argument('--no_skip_initial', dest='skip_initial', help='Skip uncached starting measurements', action='store_false')

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
        f.stats(args.data, args.partition, args.extension, args.amount, args.kind, args.readbuffer, args.skip_initial)
    elif args.subcommand == 'merge':
        merge(args.data)
    else:
        parser.print_help()