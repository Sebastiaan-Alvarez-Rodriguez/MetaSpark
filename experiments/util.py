import os
import subprocess

from dynamic.metadeploy import MetaDeployState
from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.printer import *

# Returns number of results in given output log location
def check_num_results(outputloc):
    command = 'wc -l "{}"'.format(outputloc)
    try:
        return int(subprocess.check_output(command, shell=True).decode('utf-8').split(' ')[0])
    except Exception as e:
        return 0

# Function to coordinate blocking
# We block while we have not lines_needed lines in the output log
def blockfunc(metadeploy, outputloc, lines_needed):
    numlines = check_num_results(outputloc)
    metadeploy.eprint('PING: Have {}/{} results for loc {}'.format(numlines, lines_needed, outputloc))
    if numlines >= lines_needed:
        return MetaDeployState.COMPLETE, numlines
    else:
        return MetaDeployState.BUSY, numlines


# Deploys all data from /local to /dev/shm. 
# If the data is not in /local, we deploy data from NFS mount to /local first and then proceed.
# This is indeed a seemingly unnecessary step. However, next time we run this function,
# we can deploy data straight from /local, which should be lots faster than copying from NFS mount again.
def deploy_data_fast(metadeploy, reservation, generate_cmd, node, extension, amount, kind, partitions_per_node, extension_filepath, amounts_multiplier=1):
    clean_cmd = 'rm -rf "{}" > /dev/null 2>&1'.format(loc.get_node_raw_ram_dir())
    if not metadeploy.deploy_nonspark_application(reservation, clean_cmd):
        # There were some files that we could not remove, possibly permission issues. Just go on
        printw('Could not "{}" on all nodes!'.format(clean_cmd))

    # Generate to /local
    if not metadeploy.deploy_nonspark_application(reservation, generate_cmd):
        raise RuntimeError('!! Fatal error when trying to deploy data using application (command used: "{}")'.format(generate_cmd))

    # make directories on remotes
    mkdir_cmd = 'mkdir -p "{}"'.format(fs.join(loc.get_node_data_dir(DeployMode.RAM), amount, node*partitions_per_node))
    if not metadeploy.deploy_nonspark_application(reservation, mkdir_cmd):
        raise RuntimeError('!! Fatal error when trying to mkdir on RAM (command used: "{}")'.format(mkdir_cmd))
        

    # copy data to RAM directory
    frompath = fs.join(loc.get_node_data_dir(DeployMode.LOCAL), amount, node*partitions_per_node, extension_filepath)
    topath = fs.join(loc.get_node_data_dir(DeployMode.RAM), amount, node*partitions_per_node)
    cp_cmd = 'cp -r "{}" "{}"'.format(frompath, topath)
    if not metadeploy.deploy_nonspark_application(reservation, cp_cmd):
        raise RuntimeError('!! Fatal error when trying to cp data to RAM (command used: "{}")'.format(cp_cmd))
    if (amounts_multiplier > 1):
        ln_cmd = 'python3 {}/main.py deploy multiplier -n {} -d "{}" -e {}'.format(fs.abspath(), amounts_multiplier, fs.join(topath, extension_filepath), extension)
        if not metadeploy.deploy_nonspark_application(reservation, ln_cmd):
            raise RuntimeError('!! Fatal error when trying to create symlinks in RAM')
           
    prints('Deployed data successfully to "{}"'.format(topath))
    return True
