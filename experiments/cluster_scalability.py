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

class ClusterScalabilityExperiment(ExperimentInterface):
    '''Data scalability experiment'''

    class BenchmarkClusterScalability(base.BenchmarkBase):
        def __init__(self):
            # Application deployment params
            self.resultloc = fs.join(fs.abspath(), '..', 'cluster_scalability_res')

            # Experiment params
            self.nodes = [4, 8, 16, 32]


    # Start experiment with set parameters
    def start(self, metadeploy):
        b = BenchmarkClusterScalability()
        metadeploy.eprint('Ready to deploy!')
        b.iterate_experiments(self, metadeploy)


    def stop(self, metadeploy):
        return True