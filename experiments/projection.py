from experiments.interface import ExperimentInterface
import experiments.benchmark_base as base
import experiments.util as eu
from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.printer import *

def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return ProjectionExperiment


class BenchmarkProjection(base.BenchmarkBase):
    '''Class overriding default configurations'''
    def __init__(self):
        super(BenchmarkProjection, self).__init__()
        # Application deployment params
        self.resultloc = fs.join(fs.abspath(), '..', 'projection_res')

        # Data deployment params
        self.num_columns = 100

        # Experiment params
        self.amount = 20000000 # For this experiment, we generate 30x less rows, to compensate for the x25 columns
        self.amount_multipliers = [8] # makes number of rows this factor larger using symlinks
        self.compute_columns = [10, 20, 50, 100]


class ProjectionExperiment(ExperimentInterface):
    '''Projection experiment'''

    # Start experiment with set parameters
    def start(self, metadeploy):
        b = BenchmarkProjection()
        metadeploy.eprint('Ready to deploy!')
        b.iterate_experiments(metadeploy)


    def stop(self, metadeploy):
        return True