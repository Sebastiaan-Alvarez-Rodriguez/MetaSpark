# This file contains code to generate a small config file,
# containing cluster options.


import configparser

import util.fs as fs
import util.location as loc
from util.printer import *
import util.ui as ui

def get_metaspark_cluster_conf_dir():
    return fs.join(loc.get_metaspark_conf_dir(), 'cluster')

# Check if all required data is present in the config
def validate_settings(config_loc):
    d = dict()
    d['CLUSTER'] = {'nodes', 'affinity', 'infiniband'}
    
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
    Simple object to quickly interact with stored settings.
    This way, we don't have to read in the config every time,
    or pass it along a large amount of times.
    Below, we define a global instance.
    '''
    def __init__(self, path=None):
        if path != None:
            self.picked = path
        else:
            if not fs.isdir(get_metaspark_cluster_conf_dir()):
                cfg_paths = []
            else:
                cfg_paths = [x for x in fs.ls(get_metaspark_cluster_conf_dir(), only_files=True, full_paths=True) if x.endswith('.cfg')]

            self.picked = ''
            if len(cfg_paths) == 0:
                self.picked = gen_config()
            elif len(cfg_paths == 1):
                self.picked = cfg_paths[0]
            else:
                idx = ui.ask_pick('Which Cluster config to use?', [fs.basename(x) for x in cfg_paths])
                self.picked = cfg_paths[idx]
        validate_settings(picked)
        self.parser = configparser.ConfigParser()
        self.parser.read(loc)


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

    # Persist current settings
    def persist():
        with open(config_loc, 'w') as file:
            parser.write(file)


# Import settings_instance if you wish to read settings
cfg_cluster_instance = ClusterConfig()
