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


# Deploy data. Assumes data deployment is on non-volatile storage
def deploy_data_regular(metadeploy, reservation, generate_cmd, ds_name, node, partitions_per_node, extension, amount, amount_multiplier, num_columns, extension_filepath, deploymode):
    if deploymode == DeployMode.STANDARD:
        raise NotImplementedError
    elif deploymode == DeployMode.LOCAL or deploymode == DeployMode.LOCAL_SSD: # We are dealing with LOCAL or LOCAL_SSD options.
        if not metadeploy.deploy_nonspark_application(reservation, generate_cmd):
            raise RuntimeError('!! Fatal error when trying to deploy data using application (command used: "{}")'.format(generate_cmd))
        path = fs.join(loc.get_node_data_dir(deploymode), ds_name, num_columns)
    else:
        raise NotImplementedError('Did not implement deploymode "{}"'.format(deploymode))

    if amount_multiplier != 1:
        # make directories on remotes
        frompath = fs.join(loc.get_node_data_dir(deploymode), ds_name, num_columns, amount, node*partitions_per_node, extension_filepath)
        topath = fs.join(loc.get_node_data_link_dir(deploymode), ds_name, num_columns, amount, node*partitions_per_node, extension_filepath)
        rm_cmd = 'rm -rf "{}"'.format(topath)
        if not metadeploy.deploy_nonspark_application(reservation, rm_cmd):
            raise RuntimeError('!! Fatal error when trying to rm -rf the linkdir (command used: "{}")'.format(rm_cmd))

        mkdir_cmd = 'mkdir -p "{}"'.format(topath)
        if not metadeploy.deploy_nonspark_application(reservation, mkdir_cmd):
            raise RuntimeError('!! Fatal error when trying to mkdir on /local (command used: "{}")'.format(mkdir_cmd))
    
        # Place initial set of hardlinks for the 'base' dataset
        ln_init_cmd = 'ln -fn "{}/*" "{}/"'.format(frompath, topath)
        if not metadeploy.deploy_nonspark_application(reservation, ln_init_cmd):
            raise RuntimeError('!! Fatal error when trying to link base dataset (command used: "{}")'.format(ln_init_cmd))
        
        # place hardlinks to inflate data size by given multiplier
        ln_cmd = 'python3 {}/main.py deploy multiplier -n {} -d "{}" -e {}'.format(fs.abspath(), amount_multiplier, topath, extension)
        if not metadeploy.deploy_nonspark_application(reservation, ln_cmd):
            raise RuntimeError('!! Fatal error when trying to create symlinks for dataset')
        return fs.join(loc.get_node_data_link_dir(deploymode), ds_name, num_columns)
    else: # We do not expand the original dataset, so we can read directly from dataset source
        return path


# Deploys all data from /local to /dev/shm. 
# If the data is not in /local, we deploy data from NFS mount to /local first and then proceed.
# This is indeed a seemingly unnecessary step. However, next time we run this function,
# we can deploy data straight from /local, which should be lots faster than copying from NFS mount again.
def deploy_data_triangle(metadeploy, reservation, generate_cmd, dataset_name, node, partitions_per_node, extension, amount, amount_multiplier, num_columns, extension_filepath):
    clean_cmd = 'rm -rf "{}" > /dev/null 2>&1'.format(loc.get_node_ram_dir())
    if not metadeploy.deploy_nonspark_application(reservation, clean_cmd):
        # There were some files that we could not remove, possibly permission issues. Just go on
        printw('Could not "{}" on all nodes!'.format(clean_cmd))

    # Generate to /local
    if not metadeploy.deploy_nonspark_application(reservation, generate_cmd):
        raise RuntimeError('!! Fatal error when trying to deploy data using application (command used: "{}")'.format(generate_cmd))

    # make directories on remotes
    mkdir_cmd = 'mkdir -p "{}"'.format(fs.join(loc.get_node_data_dir(DeployMode.RAM), dataset_name, num_columns, amount, node*partitions_per_node))
    if not metadeploy.deploy_nonspark_application(reservation, mkdir_cmd):
        raise RuntimeError('!! Fatal error when trying to mkdir on RAM (command used: "{}")'.format(mkdir_cmd))

    # copy data to RAM directory
    frompath = fs.join(loc.get_node_data_dir(DeployMode.LOCAL), dataset_name, num_columns, amount, node*partitions_per_node, extension_filepath)
    topath = fs.join(loc.get_node_data_dir(DeployMode.RAM), dataset_name, num_columns, amount, node*partitions_per_node)
    cp_cmd = 'cp -r "{}" "{}"'.format(frompath, topath)
    if not metadeploy.deploy_nonspark_application(reservation, cp_cmd):
        raise RuntimeError('!! Fatal error when trying to cp data to RAM (command used: "{}")'.format(cp_cmd))

    # place hardlinks to inflate data size by given multiplier
    if (amount_multiplier > 1):
        ln_cmd = 'python3 {}/main.py deploy multiplier -n {} -d "{}" -e {}'.format(fs.abspath(), amount_multiplier, fs.join(topath, extension_filepath), extension)
        if not metadeploy.deploy_nonspark_application(reservation, ln_cmd):
            raise RuntimeError('!! Fatal error when trying to create symlinks in RAM')

    prints('Deployed data successfully to "{}"'.format(topath))
    return fs.join(loc.get_node_data_dir(DeployMode.RAM), dataset_name, num_columns)
        
