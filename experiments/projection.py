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
        self.eventlog_path = '/home/salvarez/spark-persist'
        # self.submit_opts = ' \
        #     --conf \'spark.sql.shuffle.partitions={}\''.format(8*16*4)
        # --conf \'spark.sql.sources.ignoreDataLocality.enabled=true\''
        # '--conf \'spark.locality.wait=0\' \
        #  --conf \'spark.locality.wait.process=0\''


        # Data deployment params
        self.num_columns = 100

        # Experiment params
        self.amount = 30000000 # For this experiment, we generate 20x less rows, to compensate for the x25 columns
        self.amount_multipliers = [64]
        # self.nodes = [16]
        self.compute_columns = [10, 20, 50, 100]
        # self.test_modes = ['--arrow-only']


class ProjectionExperiment(ExperimentInterface):
    '''Projection experiment'''

    # Start experiment with set parameters
    def start(self, metadeploy):
        b = BenchmarkProjection()
        metadeploy.eprint('Ready to deploy!')
        b.iterate_experiments(metadeploy)


    def stop(self, metadeploy):
        return True

    def max_nodes_needed(self):
        return max(self.nodes)