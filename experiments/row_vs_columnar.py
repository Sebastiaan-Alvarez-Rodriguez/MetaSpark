from experiments.interface import ExperimentInterface
import experiments.benchmark_base as base
import experiments.util as eu
from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.printer import *

def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return RowColumnExperiment

class RowColumnExperiment(ExperimentInterface):
    '''Row vs column format experiment'''

    class BenchmarkRowColumn(base.BenchmarkBase):
        def __init__(self):
            # Cluster spawning params
            self.reserve_time = '12:00:00'

            # Application deployment params
            self.resultloc = fs.join(fs.abspath(), '..', 'row_vs_columnar_res')

            # Experiment params
            self.amount_multipliers = [4,8,16,32] # makes number of rows this factor larger using symlinks
            self.extensions = ['pq', 'csv']


    # Start experiment with set parameters
    def start(self, metadeploy):
        b = BenchmarkRowColumn()
        metadeploy.eprint('Ready to deploy!')
        b.iterate_experiments(self, metadeploy)


    def stop(self, metadeploy):
        return True