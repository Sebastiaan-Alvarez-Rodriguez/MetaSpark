from experiments.interface import ExperimentInterface
import experiments.benchmark_base as base
import experiments.util as eu
from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.printer import *

def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return ClusterScalabilityExperiment

class BenchmarkClusterScalability(base.BenchmarkBase):
        '''Class overriding default configurations'''
        def __init__(self):
            super(BenchmarkClusterScalability, self).__init__()
            # Application deployment params
            self.resultloc = fs.join(fs.abspath(), '..', 'cluster_scalability_res')

            # Experiment params
            self.nodes = [4, 8, 16, 32]

class ClusterScalabilityExperiment(ExperimentInterface):
    '''Cluster scalability experiment'''

    # Start experiment with set parameters
    def start(self, metadeploy):
        b = BenchmarkClusterScalability()
        metadeploy.eprint('Ready to deploy!')
        b.iterate_experiments(metadeploy)


    def stop(self, metadeploy):
        return True