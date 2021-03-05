import argparse

from config.meta import cfg_meta_instance as metacfg
from remote.util.deploymode import DeployMode
from util.executor import Executor
import util.fs as fs
import util.location as loc
from util.printer import *


'''File containing data deployment CLI'''


def deploy_data(reservation_or_number, datalist, deploy_mode, skip, subpath=''):
    print('Synchronizing data to local nodes...')
    data = ' '.join(datalist)
    from remote.reserver import reservation_manager
    try:
        reservation = reservation_manager.get(reservation_or_number) if isinstance(reservation_or_number, int) else reservation_or_number
        if not reservation.validate():
            printe('Reservation no longer valid. Cannot deploy data: {}'.format(', '.join(datalist)))
            return False
    except FileNotFoundError as e:
        printe('No reservation found on remote. Cannot run!')
        return False
    except Exception as e:
        printe('Reservation file found, no longer active')
        return False

    if deploy_mode == DeployMode.STANDARD: 
        # We already collected the data in our data dir on the NFS mount, so no need to copy again
        state = True
    else:
        target_dir = fs.join(loc.get_node_data_dir(deploy_mode), subpath)

        mkdir_executors = []
        executors = []
        for host in reservation.deployment.nodes:
            mkdir_executors.append(Executor('ssh {} "mkdir -p {}"'.format(host, target_dir), shell=True))

            command = 'rsync -az {} {}'.format(data, target_dir)
            command+= ' --exclude '+' --exclude '.join(['.git', '__pycache__', '*.crc'])
            if skip:
                command+= ' --ignore-existing'
            executors.append(Executor('ssh {} "{}"'.format(host, command), shell=True))
        Executor.run_all(mkdir_executors)
        state = Executor.wait_all(mkdir_executors, stop_on_error=False)
        Executor.run_all(executors)
        state &= Executor.wait_all(executors, stop_on_error=False)
    if state:
        prints('Data deployment success!')
    else:
        printw('Data deployment failure on some nodes!')
    return state
    

def deploy_data_remote(reservation_number, datalist, deploy_mode, skip):
    for location in datalist:
        glob_locs = glob.glob(location)
        for glob_loc in glob_locs:
            if not fs.exists(glob_loc):
                printe('Path "{}" does not exist'.format(glob_loc))
                return False

    print('Synchronizing data to server...')
    data = ' '.join(datalist)
    command = 'rsync -az {} {}:{}'.format(data, metacfg.ssh.ssh_key_name, loc.get_remote_metaspark_data_dir())
    command+= ' --exclude '+' --exclude '.join(['.git', '__pycache__'])
    if skip:
        command+= '--ignore-existing'
    if os.system(command) == 0:
        print('Data sync success!')
    else:
        printe('Data sync failure!')
        return False
    
    remote_datalist = [fs.join(loc.get_remote_metaspark_data_dir(), x) for x in datalist]
    program = '{} {} --deploy-mode {} {}'.format(reservation_number, ' '.join(remote_datalist), deploy_mode, '--skip' if skip else '')
    command = 'ssh {} "python3 {}/main.py deploy data {}"'.format(metacfg.ssh.ssh_key_name, loc.get_remote_metaspark_dir(), program)
    print('Connecting using key "{}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0

'''
Multiplies the dataset by a given integer factor.
We multiply a dataset by factor X by placing X-1 hardlinks in every directory.
'''
def deploy_data_multiplier(multiplier, directory, extension):
    if directory[-1] == fs.sep():
        directory = directory[:-1]
    num_files = int(fs.basename(fs.dirname(directory)))
    for x in range(num_files):
        source = fs.join(directory, '{}.{}'.format(x, extension))
        for y in range(multiplier-1):
            dest = fs.join(directory, '{}_{}.{}'.format(x, y, extension))
            try:
                fs.ln(source, dest, soft=False, is_dir=False)
            except FileExistsError as e:
                continue # Ignore when symlink already exists
    return True


# Register 'deploy' subparser modules
def subparser(subsubparsers):
    deploydataparser = subsubparsers.add_parser('data', help='Deploy data (use deploy data -h to see more...)')
    deploydataparser.add_argument('reservation', help='Reservation number of cluster to deploy on.', type=int)
    deploydataparser.add_argument('data', nargs='+', metavar='file', help='Files to place on reserved nodes local drive.')
    deploydataparser.add_argument('-dm', '--deploy-mode', type=str, metavar='mode', default=str(DeployMode.STANDARD), help='Deployment mode for data.', choices=[str(x) for x in DeployMode])
    deploydataparser.add_argument('--skip', help='Skip data if already found on nodes.', action='store_true')
    deploydataparser.add_argument('--remote', help='Indicates we are not currently on DAS. Deploy experiments on remote over SSH.', action='store_true')

    deploymultiplierparser = subsubparsers.add_parser('multiplier', help='Data-utility to increase data size virtually, by applying hardlinks.')
    deploymultiplierparser.add_argument('-n', '--number', type=int, metavar='amount', default='10', help='Data multiplier to apply.')
    deploymultiplierparser.add_argument('-d', '--dir', type=str, metavar='path', help='Dir to perform file multiplication.')
    deploymultiplierparser.add_argument('-e', '--extension', type=str, metavar='extension', help='Extension of files in dir.')
    return deploydataparser, deploymultiplierparser

# Return True if we found arguments used from this subsubparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function
def deploy_args_set(args):
    return args.subcommand == 'data' or args.subcommand == 'multiplier'


def deploy(parsers, args):
    deploydataparser = parsers
    if args.subcommand == 'data':
        if args.remote:
            return deploy_data_remote(args.reservation, args.data, args.deploy_mode, args.skip)
        else:
            return deploy_data(args.reservation, args.data, args.deploy_mode, args.skip)
    elif args.subcommand == 'multiplier':
        deploy_data_multiplier(args.number, args.dir, args.extension)
    