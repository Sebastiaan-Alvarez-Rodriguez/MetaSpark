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
def merge(data, skip_initial=True):
    def file_append(source, target):
        with open(source, 'r') as s:
            with open(target, 'a') as t:
                if skip_initial:
                    try:
                        next(s)
                        next(s)
                    except StopIteration as e:
                        return
                while True:
                    data = s.read(65536)
                    if data:
                        t.write(data)
                    else:
                        break

    datapath = fs.join(loc.get_metaspark_results_dir(), data)
    merges = 0
    for dnode in sorted(fs.ls(datapath, only_dirs=True), key=lambda x: int(x)):
        for dextension in fs.ls(fs.join(datapath, dnode), only_dirs=True):
            for damount in sorted(fs.ls(fs.join(datapath, dnode, dextension), only_dirs=True), key=lambda x: int(x)):
                for dkind in fs.ls(fs.join(datapath, dnode, dextension, damount), only_dirs=True):
                    flist = sorted([x for x in fs.ls(fs.join(datapath, dnode, dextension, damount, dkind), only_files=True, full_paths=True) if x.split('.')[-1].startswith('res')], key=lambda x: filename_to_rb(x)+len(x))
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


def _add_filter_args(parser, type_opts):
    parser.add_argument('-n', '--node', nargs='+', metavar='filter', help='Node filters')
    parser.add_argument('-p', '--partition', nargs='+', metavar='filter', help='Partitions-per-node filters')
    parser.add_argument('-e', '--extension', nargs='+', metavar='filter', help='Extension filters')
    parser.add_argument('-a', '--amount', nargs='+', metavar='filter', help='Amount filters')
    parser.add_argument('-k', '--kind', nargs='+', metavar='filter', help='Kind filters')
    parser.add_argument('-rb', '--readbuffer', nargs='+', metavar='filter', help='Readbuffer filters')
    parser.add_argument('--no_skip_initial', dest='skip_initial', help='Skip uncached starting measurements', action='store_false')
    parser.add_argument('--type', nargs='?', metavar='type', default='generic', type=str, const='generic', help='Type: '+', '.join(type_opts))

def _filterparser(subsubparsers):
    filterparser = subsubparsers.add_parser('filter', help='Display generic info, based on filters')
    _add_filter_args(filterparser, ['barplot', 'dot', 'generic', 'line', 'normal'])
    return filterparser


def _specificparser(subsubparsers):
    specificparser = subsubparsers.add_parser('specific', help='Very specific plots, using filters')
    _add_filter_args(specificparser, ['data_scalability'])
    return specificparser

# Register 'deploy' subparser modules
def subparser(subparsers):
    resultparser   = subparsers.add_parser('results', help='Create result graphs/statistics')
    subsubparsers  = resultparser.add_subparsers(help='Subsubcommands', dest='subcommand')
    filterparser   = _filterparser(subsubparsers)
    specificparser = _specificparser(subsubparsers)

    mergeparser = subsubparsers.add_parser('merge', help='Merge continuation files into main result files')
    mergeparser.add_argument('--no_skip_initial', dest='skip_initial', help='Skip uncached starting measurements', action='store_false')

    resultparser.add_argument('data', help='Location of data', type=str)
    resultparser.add_argument('-l', '--large', help='Forces to generate large graphs, with large text', action='store_true')
    resultparser.add_argument('-ns', '--no-show', dest='no_show', help='Do not show generated graph (useful on servers without xorg forwarding)', action='store_true')
    resultparser.add_argument('-s', '--store', help='Store generated graph (in {}/<resultdirname>/<graph_name>.<type>)'.format(loc.get_metaspark_graphs_dir()), action='store_true')
    resultparser.add_argument('-t', '--storetype', nargs=1, help='Preferred storage type (default=pdf)', default='pdf')
    return resultparser, subsubparsers, filterparser, specificparser, mergeparser

# Return True if we found arguments used from this subparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function 
def results_args_set(args):
    return args.command == 'results'

# Processing of result commandline args occurs here
def results(parsers, args):
    resultparser, subsubparsers, filterparser, specificparser, mergeparser = parsers

    # We explicitly MUST check if matplotlib is available to import
    # If it is not, we cannot process results on the current machine
    if not importer.library_exists('matplotlib'):
        printe('Cannot work with results. Matplotlib is not available!')
        return

    if not importer.library_exists('numpy'):
        printe('Cannot work with results. Numpy is not available!')
        return

    if args.store and args.storetype is None:
        parser.error('--store (-st) requires --storetype (-t)')
        return
    args.storetype = args.storetype[0]
    import result.util.storer as storer # We can only import storer here, as it depends on matplotlib and we don't want to check matplotlib availibility again
    if args.store and not storer.filetype_is_supported(args.storetype):
        parser.error('--storetype only supports filetypes: {} (not given: {})'.format(', '.join(storer.supported_filetypes()), args.storetype))
        return

    if not fs.isdir(loc.get_metaspark_results_dir()):
        printe('[FAILURE] You have no experiment results directory "{}". Run experiments to get some data first.'.format(log.get_metaspark_results_dir()))
    fargs = [args.large, args.no_show, args.store, args.storetype]


    if args.subcommand == 'filter':
        fdata = [args.node, args.partition, args.extension, args.amount, args.kind, args.readbuffer]
        if args.type == 'barplot':
            import result.filter.barplot as b
            b.stats(args.data, *(fdata+fargs+[args.skip_initial]))
        elif args.type == 'dot':
            import result.filter.dot as d
            d.stats(args.data, *(fdata+fargs+[args.skip_initial]))
        elif args.type == 'generic':
            import result.filter.generic as g
            g.stats(args.data, *(fdata+[args.skip_initial]))
        elif args.type == 'line':
            import result.filter.line as l
            l.stats(args.data, *(fdata+fargs+[args.skip_initial]))    
        elif args.type == 'normal':
            import result.filter.normal as n
            n.stats(args.data, *(fdata+fargs+[args.skip_initial]))
        else:
            filterparser.print_help()
    elif args.subcommand == 'specific':
        fdata = [args.node, args.partition, args.extension, args.amount, args.kind, args.readbuffer]
        if args.type == 'data_scalability':
            import result.filter.specific.data_scalability as p
            p.stats(args.data, *(fdata+fargs+[args.skip_initial]))
        elif args.type == 'cluster_scalability':
            import result.filter.specific.cluster_scalability as c
            c.stats(args.data, *(fdata+fargs+[args.skip_initial]))    
        else:
            specificparser.print_help()
    elif args.subcommand == 'merge':
        merge(args.data, args.skip_initial)
    else:
        resultparser.print_help()