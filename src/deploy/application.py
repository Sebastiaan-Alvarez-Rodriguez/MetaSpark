import os

from config.meta import cfg_meta_instance as metacfg
from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.printer import *
import util.time as tm

'''File containing application deployment CLI'''


# Replace directories
def _args_replace(args, timestamp, no_result=False):    
    tmp1 = args if no_result else args.replace('[[RESULTDIR]]', fs.join(loc.get_metaspark_results_dir(), timestamp))
    tmp1 = tmp1.replace('[[DATA-STANDARDDIR]]', loc.get_node_data_dir(DeployMode.STANDARD))
    tmp1 = tmp1.replace('[[DATA-LOCALDIR]]', loc.get_node_data_dir(DeployMode.LOCAL))
    tmp1 = tmp1.replace('[[DATA-LOCAL-SSDDIR]]', loc.get_node_data_dir(DeployMode.LOCAL_SSD))
    return tmp1.replace('[[DATA-RAMDIR]]', loc.get_node_data_dir(DeployMode.RAM))


# Deploy application on a running cluster, with given rerservation
def _deploy_application_internal(reservation, jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir):
    if not reservation.validate():
        printe('Reservation no longer valid. Cannot deploy application')
        return False
    scriptloc = fs.join(loc.get_spark_bin_dir(), 'spark-submit')
    master_url = reservation.deployment.master_url
    if no_resultdir:
        driver_opts = '-Dlog4j.configuration=file:{}'.format(fs.join(loc.get_metaspark_log4j_conf_dir(),'driver_log4j.properties'))
        timestamp = None
    else:
        timestamp = tm.timestamp('%Y-%m-%d_%H:%M:%S.%f')
        fs.mkdir(loc.get_metaspark_results_dir(), timestamp)
        driver_opts = '-Dlog4j.configuration=file:{} -Doutputlog={}'.format(
            fs.join(loc.get_metaspark_log4j_conf_dir(),'driver_log4j.properties'),
            fs.join(loc.get_metaspark_results_dir(), timestamp, 'spark.log'))
        print('Output log can be found at {}'.format(fs.join(loc.get_metaspark_results_dir(), timestamp)))

    args = _args_replace(args, timestamp, no_result=no_resultdir)
    submit_opts = _args_replace(submit_opts, timestamp, no_result=no_resultdir)

    if len(extra_jars) > 0:
        extra_jars = ','.join([fs.join(loc.get_metaspark_jar_dir(), x) for x in extra_jars.split(' ')])+','
    command = '{}\
    --driver-java-options "{}" \
    --class {} \
    --jars "{}" \
    --conf spark.driver.extraClassPath={} \
    --conf spark.executor.extraClassPath={} \
    {} \
    --master {} \
    --deploy-mode cluster {} {}'.format(
        scriptloc,
        driver_opts,
        mainclass,
        extra_jars,
        fs.join(loc.get_metaspark_jar_dir(), jarfile),
        fs.join(loc.get_metaspark_jar_dir(), jarfile),
        submit_opts,
        master_url,
        fs.join(loc.get_metaspark_jar_dir(), jarfile),
        args)

    print('Executing command: {}'.format(command))
    status = os.system(command) == 0
    if status:
        prints('Deployment was successful!')
    else:
        printe('There were errors during deployment.')
    return status


def deploy_application(reservation_number, jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir):
    if args == None:
        args = ''
    if extra_jars == None:
        extra_jars = ''
    if submit_opts == None:
        submit_opts = ''
    from remote.reserver import reservation_manager
    try:
        reservation = reservation_manager.get(reservation_number)
    except FileNotFoundError as e:
        printe('No reservation found on remote. Cannot run!')
        return False
    except Exception as e:
        printe('Reservation file found, no longer active')
        return False
    return _deploy_application_internal(reservation, jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir)


def deploy_application_remote(jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir):
    fs.mkdir(loc.get_metaspark_jar_dir(), exist_ok=True)
    if not fs.isfile(loc.get_metaspark_jar_dir(), jarfile):
        printw('Provided jarfile "{}" not found at "{}"'.format(jarfile, loc.get_metaspark_jar_dir()))
        while True:
            options = [fs.basename(x) for x in fs.ls(loc.get_metaspark_jar_dir(), only_files=True, full_paths=True) if x.endswith('.jar')]
            if len(options)== 0: print('Note: {} seems to be an empty directory...'.format(loc.get_metaspark_jar_dir()))
            idx = ui.ask_pick('Pick a jarfile: ', ['Rescan {}'.format(loc.get_metaspark_jar_dir())]+options)
            if idx == 0:
                continue
            else:
                jarfile = options[idx-1]
                break

    print('Synchronizing jars to server...')
    command = 'rsync -az {} {}:{}'.format(loc.get_metaspark_jar_dir(), metacfg.ssh.ssh_key_name, loc.get_remote_metaspark_dir())
    command+= ' --exclude '+' --exclude '.join(['.git', '__pycache__'])
    if os.system(command) == 0:
        prints('Export success!')
    else:
        printe('Export failure!')
        return False

    program = '{} {} {} --args \'{}\' --jars \'{}\' --opts \'{}\''.format(reservation_number, jarfile, mainclass, args, extra_jars, submit_opts)
    program += ' --no-resultdir' if no_resultdir else '' 
    command = 'ssh {} "python3 {}/main.py deploy application {}"'.format(metacfg.ssh.ssh_key_name, loc.get_remote_metaspark_dir(), program)
    print('Connecting using key "{}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0


# Register 'deploy' subparser modules
def subparser(subsubparsers):
    deployapplparser = subsubparsers.add_parser('application', help='Deploy applications (use deploy start -h to see more...)')
    deployapplparser.add_argument('reservation', help='Reservation number of cluster to deploy on.', type=int)
    deployapplparser.add_argument('jarfile', help='Jarfile to deploy.')
    deployapplparser.add_argument('mainclass', help='Main class of jarfile.')
    deployapplparser.add_argument('--args', nargs='+', metavar='argument', help='Arguments to pass on to your jarfile.')
    deployapplparser.add_argument('--jars', nargs='+', metavar='argument', help='Extra jars to pass along your jarfile.')
    deployapplparser.add_argument('--opts', nargs='+', metavar='argument', help='Extra arguments to pass on to spark-submit.')    
    deployapplparser.add_argument('--no-resultdir', dest='no_resultdir', help='Do not make a resultdirectory in <project root>/results/ for this deployment.', action='store_true')
    deployapplparser.add_argument('--remote', help='Deploy on remote machine.', action='store_true')
    return deployapplparser

# Return True if we found arguments used from this subsubparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function
def deploy_args_set(args):
    return args.subcommand == 'application'


def deploy(parsers, args):
    deployapplparser = parsers[0]
    jarfile = args.jarfile
    mainclass = args.mainclass
    jargs = ' '.join(args.args) if args.args != None else ''
    extra_jars = ' '.join(args.jars) if args.jars != None else ''
    submit_opts = ' '.join(args.opts) if args.opts != None else ''
    if args.remote:
        return deploy_application_remote(args.reservation, jarfile, mainclass, jargs, extra_jars, submit_opts, args.no_resultdir)
    else:
        return deploy_application(args.reservation, jarfile, mainclass, jargs, extra_jars, submit_opts, args.no_resultdir)
