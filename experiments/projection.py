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
        # self.flamegraph_time = '20s'
        # self.flamegraph_only_master = False
        # self.flamegraph_only_worker = False
        # self.eventlog_path = '/home/salvarez/spark-persist'

        # Data deployment params
        self.num_columns = 100

        # Experiment params
        self.amount = 30000000 # For this experiment, we generate 20x less rows, to compensate for the x25 columns
        self.amount_multipliers = [32]
        self.compute_columns = [100, 50, 20, 10]
        self.rbs = [32768]
        self.test_modes = ['--spark-only','--arrow-only']
        
        # def custom_rb_func(num_columns, compute_column, node, partitions_per_node, extension, compression, amount, amount_multiplier, kind, rb):
        #     return (256*1024) // (compute_column*8)
        # self.custom_rb_func = custom_rb_func


class ProjectionExperiment(ExperimentInterface):
    '''Projection experiment'''

    # Start experiment with set parameters
    def start(self, metadeploy):
        b = BenchmarkProjection()
        metadeploy.eprint('Ready to deploy!')
        return b.iterate_experiments(metadeploy)


    def stop(self, metadeploy):
        return True

    def max_nodes_needed(self):
        return max(self.nodes)