from experiments.interface import ExperimentInterface
import experiments.benchmark_base as base
import experiments.util as eu
from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.printer import *

def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return DataScalabilityExperiment


class BenchmarkDataScalability(base.BenchmarkBase):
    '''Class overriding default configurations'''
    def __init__(self):
        super(BenchmarkDataScalability, self).__init__()
        # Application deployment params
        self.resultloc = fs.join(fs.abspath(), '..', 'data_scalability_res')

        # Experiment params
        self.amount_multipliers = [4, 8, 16, 32, 64, 128, 256] # makes number of rows this factor larger using symlinks
        self.appl_dead_after_tries = 28

class DataScalabilityExperiment(ExperimentInterface):
    '''Data scalability experiment'''

    # Start experiment with set parameters
    def start(self, metadeploy):
        b = BenchmarkDataScalability()
        metadeploy.eprint('Ready to deploy!')
        return b.iterate_experiments(metadeploy)


    def stop(self, metadeploy):
        return True


    def max_nodes_needed(self):
        return max(self.nodes)