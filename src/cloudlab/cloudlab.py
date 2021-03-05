
'''File containing cloudlab deployment CLI'''


def _get_modules():
    import deploy.allocated as allocated
    import deploy.application as application
    import deploy.data as data
    import deploy.flamegraph as flamegraph
    import deploy.meta as meta
    import deploy.util as deployutil
    return [allocated, application, data, flamegraph, meta, deployutil]


# Register 'cloudlab' subparser modules
def subparser(subparsers):
    deployparser = subparsers.add_parser('cloudlab', help='Deploy cloudlab backends (use deploy -h to see more...)')
    subsubparsers = deployparser.add_subparsers(help='Subsubcommands', dest='subcommand')
    return [x.subparser(subsubparsers) for x in _get_modules()]


# Return True if we found arguments used from this subparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function 
def deploy_args_set(args):
    return args.command == 'cloudlab'


# Processing of deploy commandline args occurs here
def deploy(parsers, args):
    for parsers_for_module, module in zip(parsers, _get_modules()):
        if module.deploy_args_set(args):
            return module.deploy(parsers_for_module, args)
    deployparser.print_help()
    return True