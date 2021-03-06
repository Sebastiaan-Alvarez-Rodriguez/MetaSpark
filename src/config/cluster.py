# This file contains code to generate a small config file,
# containing cluster options.


import configparser

import util.fs as fs
import util.location as loc
from util.printer import *
import util.ui as ui

def ask_nodes():
    return ui.ask_int('How many physical nodes to allocate for this cluster?')

def ask_affinity(nodes):
    while True:
        ans = ui.ask_int('How many processes to launch per node in the cluster? (must be divisible by #nodes ({}))'.format(nodes))
        if ans % nodes != 0:
            printe('{0} is not divisible by {1} ({0}%{1}={2})'.format(ans, nodes, ans % nodes))
        else:
            return ans

def ask_infiniband():
    return ui.ask_bool('Use infiniband connection between the servers for communication?')


# Generate a config by asking the user relevant questions
def gen_config():
    nodes = ask_nodes()
    affinity = ask_affinity(nodes)
    infiniband = ask_infiniband()
    while True:
        configloc = fs.join(loc.get_metaspark_cluster_conf_dir(), fs.basename(ui.ask_string('Please give a name to this configuration')))
        if not configloc.endswith('.cfg'):
            configloc += '.cfg'
        if (not fs.isfile(configloc)) or ui.ask_bool('Config "{}" already exists, override?').format(configloc):
            write_config(configloc, nodes, affinity, infiniband)
            return configloc
        else:
            printw('Pick another configname.')


# Persist a configuration to file using given variables
def write_config(configloc, nodes, coallocation_affinity, infiniband):
    fs.mkdir(loc.get_metaspark_cluster_conf_dir(), exist_ok=True)
    parser = configparser.ConfigParser()
    parser['Cluster'] = {
        'nodes': nodes,
        'coallocation_affinity': coallocation_affinity,
        'infiniband': infiniband
    }
    with open(configloc, 'w') as file:
        parser.write(file)


# Check if all required data is present in the config
def validate_settings(config_loc):
    d = dict()
    d['Cluster'] = {'nodes', 'coallocation_affinity', 'infiniband'}
    
    parser = configparser.ConfigParser()
    parser.read(config_loc)
    for key in d:
        if not key in parser:
            raise RuntimeError('Missing section "{}"'.format(key))
        else:
            for subkey in d[key]:
                if not subkey in parser[key]:
                    raise RuntimeError('Missing key "{}" in section "{}"'.format(subkey, key))


class ClusterConfig(object):    
    '''
    Object to store cluster configuration settings.
    This way, we do not have to ask the user every run what cluster
    size they want.
    '''
    def __init__(self, path):
        validate_settings(path)
        self.parser = configparser.ConfigParser()
        self.parser.read(path)
        self._path = path

    # Size of our cluster (in nodes, each node has coallocation_affinity processes)
    @property
    def nodes(self):
        return int(self.parser['Cluster']['nodes'])

    # Amount of processes per node
    @property
    def coallocation_affinity(self):
        return int(self.parser['Cluster']['coallocation_affinity'])

    # True if nodes use infiniband communication, False otherwise
    @property
    def infiniband(self):
        return self.parser['Cluster']['infiniband'] == 'True'

    @property
    def path(self):
        return self._path


    # Persist current settings
    def persist():
        with open(config_loc, 'w') as file:
            parser.write(file)


# Gets a cluster config to use. Asks user if multiple candidates exist
# Returns cluster config, and a boolean describing whether we should export conf data or not
def get_cluster_config():
    fs.mkdir(loc.get_metaspark_cluster_conf_dir(), exist_ok=True)

    cfg_paths = [x for x in fs.ls(loc.get_metaspark_cluster_conf_dir(), only_files=True, full_paths=True) if x.endswith('.cfg')]
    if len(cfg_paths) == 0: #Build a cluster config if none exist
        path = gen_config()
        return ClusterConfig(path), True
    else:
        idx = ui.ask_pick('Which cluster-config to load?', ['Generate new config']+[fs.basename(x) for x in cfg_paths])
        if idx == 0:
            path = gen_config()
            return ClusterConfig(path), True
        return ClusterConfig(cfg_paths[idx-1]), False

# Load a cluster config with given filename from disk and return it
def load_cluster_config(config_filename):
    return ClusterConfig(fs.join(loc.get_metaspark_cluster_conf_dir(), config_filename))