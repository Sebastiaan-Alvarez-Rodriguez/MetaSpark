import os
import subprocess
import time

from experiments.interface import ExperimentInterface
import experiments.util as eu
from dynamic.metadeploy import MetaDeployState
from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.printer import *


def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return Cleaner



class Cleaner(ExperimentInterface):
    '''Quick experiment to clean /local directories as well as can be'''

    # Provides resultdirectory location
    def resultloc(self):
        return fs.join(fs.abspath(), '..', 'bigres')



    def init_params(self):
        # Cluster spawning params
        self.reserve_time = '15:00'
        self.config = '{}.cfg'
        self.debug_mode = False # less printing
        self.cluster_deploy_mode = DeployMode.LOCAL
        self.no_interact = True # We want to run batch jobs, so no user interaction

        # Data deployment params
        self.data_deploy_modes = DeployMode.LOCAL # Location to remove data from
        self.remove_all = True
        self.remove_only_paths = [((fs.join(str(amount), str(node*partitions_per_node), str(extension) for amount in [600000000]) for node in [4]) for partitions_per_node in [16]) for extension in ['pq']]

        # Experiment params
        self.nodes = 4
        self.reservations = 200 # Number of times to make a reservation
        self.retries = 5
        self.total_nodes_amount = 68 # VU=68, LU=24



    # Start experiment with set parameters
    def start(self, metadeploy):
        self.init_params()
        metadeploy.eprint('Ready to clean!')
        visited_nodes = set()
        for x in range(self.reservations):
            metadeploy.cluster_stop()
            if len(visited_nodes) == self.total_nodes_amount:
                prints('Successfully cleaned all junk!')
                break
            if not metadeploy.cluster_start(self.reserve_time, self.config.format(self.nodes), self.debug_mode, str(self.cluster_deploy_mode), self.no_interact, launch_spark=False):
                printe('!! Fatal error when trying to start cluster !! ({})'.format(outputloc))
                return False
            raw_nodes = set(metadeploy.deployment.raw_nodes)
            new_nodes = raw_nodes - visited_nodes
            if len(new_nodes) > 0: # We have at least 1 new node when compared to before
                visited_nodes.update(new_nodes)
                if self.remove_all:
                    metadeploy.clean_junk(datadir=loc.get_node_data_dir(self.data_deploy_mode)) # Clean regular junk            
                else:
                    for appendix in self.remove_only_paths:
                        metadeploy.clean_junk(datadir=fs.join(loc.get_node_data_dir(self.data_deploy_mode), appendix)) # Clean junk, including specific datadir paths


    def stop(self, metadeploy):
        return True