# This file handles argument parsing for deployments,
# as well as actual deployment.

import argparse
import os

from config.meta import cfg_meta_instance as metacfg
import remote.util.ip as ip
import util.location as loc
import util.fs as fs
import util.ui as ui
from util.printer import *


def _deploy_internal(jarfile, mainclass, master_url, args):
    print('Got jarfile {}, exists: {}'.format(jarfile, fs.isfile(loc.get_metaspark_jar_dir(), jarfile)))
    scriptloc = fs.join(loc.get_spark_bin_dir(), 'spark-submit')
    command = '{} --class {} --master {} --deploy-mode cluster {} {}'.format(scriptloc, mainclass, master_url, fs.join(loc.get_metaspark_jar_dir(), jarfile), args)
    exit(os.system(command))


def _deploy(jarfile, mainclass, master_url, args):
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
    command = 'rsync -az {} {}:{}'.format(loc.get_metaspark_jar_dir(), metacfg.ssh.ssh_key_name, fs.join(loc.get_remote_metaspark_dir(), 'jars'))
    command+= ' --exclude '+' --exclude '.join([
        '.git',
        '__pycache__'])
    if os.system(command) == 0:
        prints('Export success!')
    else:
        printe('Export failure!')
        return False

    program = '{} {} {} --deploy_internal --args {}'.format(jarfile, mainclass, master_url, args)

    command = 'ssh {} "python3 {}/main.py deploy {}"'.format(
    metacfg.ssh.ssh_key_name,
    loc.get_remote_metaspark_dir(),
    program)
    print('Connecting using key "{}"...'.format(metacfg.ssh.ssh_key_name))
    return os.system(command) == 0


# Register 'deploy' subparser
def subparser(registrar):
    resultparser = registrar.add_parser('deploy', help='Deploy applications (use deploy -h to see more...)')
    resultparser.add_argument('jarfile', help='Jarfile to deploy')
    resultparser.add_argument('mainclass', help='Main class of jarfile')
    resultparser.add_argument('master_url', help='Master url for cluster')
    resultparser.add_argument('--args', nargs='*', help='Arguments to pass on to your jarfile')
    resultparser.add_argument('--deploy_internal', help=argparse.SUPPRESS, action='store_true')

# Return True if we found arguments used from this subparser, False otherwise
# We use this to redirect command parse output to this file, deploy() function 
def deploy_args_set(args):
    return hasattr(args, 'jarfile') or hasattr(args, 'mainclass') or hasattr(args, 'master_url') or hasattr(args, 'deploy_internal')
# Processing of deploy commandline args occurs here
def deploy(parser, args):
    jarfile = args.jarfile
    mainclass = args.mainclass
    master_url = args.master_url
    jargs = ' '.join(args.args) if args.args != None else ''

    if args.deploy_internal:
        return _deploy_internal(jarfile, mainclass, master_url, jargs)
    else:
        return _deploy(jarfile, mainclass, master_url, jargs)