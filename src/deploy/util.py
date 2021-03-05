import socket

import das.das as das


'''File containing deployment utilities CLI'''


# Returns the number of available nodes on the cluster it is executed on
def nodes_used(silent):
    found = das.nodes_used()
    if not silent:
        print('{} has {} used nodes at this moment'.format(socket.gethostname(), found))
    return found


# Returns 1 if we have an active reservation, 0 otherwise
def check_active(silent):
    found = das.have_reservation()
    if not silent:
        print('Found active reservation' if found == 1 else 'Did not find active reservation')


# Register 'deploy' subparser modules
def subparser(subsubparsers):
    utilparser = subsubparsers.add_parser('util', help='Deploy utilities')
    utilparsers = utilparser.add_subparsers(help='Subsubcommands', dest='subcommand')

    nodesusedparser = utilparsers.add_parser('nodes_used', help='Returns number of used nodes on this machine (as program exit code)')
    nodesusedparser.add_argument('-s', '--silent', help='If set, does not print anything', action='store_true')

    checkactiveparser = utilparsers.add_parser('check_active', help='Returns whether we are active on this machine (as program exit code)')
    checkactiveparser.add_argument('-s', '--silent', help='If set, does not print anything', action='store_true')
    return nodesusedparser, checkactiveparser


# Return True if we found arguments used from this subsubparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function
def deploy_args_set(args):
    return args.subcommand == 'util' or args.subcommand == 'nodes_used'


def deploy(parsers, args):
    deployflamenodeparser, deployflameparser = parsers
    if args.subcommand == 'check_active':
        return check_active(args.silent)
    elif args.subcommand == 'nodes_used':
        return nodes_used(args.silent)