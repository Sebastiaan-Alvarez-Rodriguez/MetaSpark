from experiments.interface import ExperimentInterface
import experiments.benchmark_base as base
import experiments.util as eu
from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.printer import *

def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return FrontendExperiment

class BenchmarkFrontend(base.BenchmarkBase):
    '''Class overriding default configurations'''
    def __init__(self):
        super(BenchmarkFrontend, self).__init__()
        # Cluster spawning params
        self.reserve_time = '20:00:00'

        # Application deployment params
        self.resultloc = fs.join(fs.abspath(), '..', 'frontend_res')

        # Experiment params
        self.kinds = ['df', 'df_sql', 'ds', 'rdd']


class FrontendExperiment(ExperimentInterface):
    '''Data scalability experiment'''

    # Start experiment with set parameters
    def start(self, metadeploy):
        b = BenchmarkFrontend()
        metadeploy.eprint('Ready to deploy!')
        b.iterate_experiments(metadeploy)


    def stop(self, metadeploy):
        return True

    def max_nodes_needed(self):
        return max(self.nodes)