# This file handles argument parsing for deployments,
# as well as actual deployment.

import os

import util.location as loc
import util.fs as fs
import util.ui as ui
from util.printer import *

def _deploy_internal(jarfile, mainclass, args):
    print('Got jarfile {}, exists: {}'.format(jarfile, fs.isfile(loc.get_metaspark_jar_dir(), jarfile)))
    scriptloc = fs.join(loc.get_spark_bin_dir(), 'spark-submit')
    # TODO: Get deployment master spark url and port. Either ask or use file.
    command = '{} --class {} --master spark://{}:{} --deploy-mode server {} {}'.format(scriptloc, mainclass, master_host, master_port, jarfile, args)
    return os.system(command)


def _deploy(jarfile, mainclass, args):
    if fs.isfile(loc.get_metaspark_jar_dir(), jarfile):
        jarfile = fs.join(loc.get_metaspark_jar_dir(), jarfile)
    else:
        printw('Provided jarfile not found at "{}"'.format(fs.join(loc.get_metaspark_jar_dir(), jarfile)))
        while True:
            options = [fs.basename(x) for x in fs.ls(loc.get_metaspark_jar_dir(), only_files=True, full_paths=True) if x.endswith('.jar')]
            idx = ui.ask_pick('Pick a jarfile: ', ['Rescan {}'.format(loc.get_metaspark_jar_dir())]+options)
            if idx == 0:
                continue
            else:
                jarfile = options[idx-1]
                break
        command = 'rsync -az {} {}:{}'.format(loc.get_metaspark_jar_dir(), metacfg.ssh.ssh_key_name, fs.join(loc.get_remote_metaspark_dir(), 'jars'))
        command+= ' --exclude '+' --exclude '.join([
            '.git',
            '__pycache__'])
        if os.system(command) == 0:
            prints('Export success!')
        else:
            printe('Export failure!')
            return False

        program = '--deploy_internal {} {} --args {}'.format(jarfile, mainclass, args)

        command = 'ssh {0} "python3 {1}/main.py {2}"'.format(
        metacfg.ssh.ssh_key_name,
        loc.get_remote_metaspark_dir(),
        program)
    print('Connecting using key "{0}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0


# Register 'deploy' subparser
def subparser(registrar):
    resultparser = registrar.add_parser('deploy', help='Deploy applications (use deploy -h to see more...)')
    resultparser.add_argument('jarfile', help='Jarfile to deploy')
    resultparser.add_argument('mainclass', help='Main class of jarfile')
    resultparser.add_argument('--args', nargs='*', help='Arguments to pass on to your jarfile')

# Return True if we found arguments used from this subparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function 
def deploy_args_set(args):
    return hasattr(args, 'jarfile') or hasattr(args, 'mainclass')

# Processing of deploy commandline args occurs here
def deploy(parser, args):
    jarfile = args.jarfile
    mainclass = args.mainclass
    jargs = ' '.join(args.args) if args.args != None else ''
    return _deploy(jarfile, mainclass, jargs)