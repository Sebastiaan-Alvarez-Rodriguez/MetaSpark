

def cluster_allocate():
    pass

# Register 'cloudlab' subparser modules
def subparser(subsubparsers):
    clusterparser = subsubparsers.add_parser('cluster', help='Deploy clusters on cloudlab.')
    clusterparser.add_argument('-e', '--experiment', nargs='+', metavar='experiments', help='Experiments to deploy.')
    clusterparser.add_argument('--remote', help='Indicates we are not currently on DAS. Deploy experiments on remote over SSH.', action='store_true')
    return clusterparser


# Return True if we found arguments used from this subsubparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function
def deploy_args_set(args):
    return args.subcommand == 'cluster'


def deploy(parsers, args):
    clusterparser = parsers
    if args.remote:
        return deploy_meta_remote(args.experiment)
    else:
        return deploy_meta(args.experiment)