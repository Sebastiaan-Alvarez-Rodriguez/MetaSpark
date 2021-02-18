from experiments.interface import ExperimentInterface
import experiments.benchmark_base as base
import experiments.util as eu
from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.printer import *

def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return CompressionExperiment


class BenchmarkCompression(base.BenchmarkBase):
    '''Class overriding default configurations'''
    def __init__(self):
        super(BenchmarkCompression, self).__init__()
        # Application deployment params
        self.resultloc = fs.join(fs.abspath(), '..', 'compression_res')

        # Experiment params
        self.amount_multipliers = [4, 8, 16, 32]
        self.compressions = ['gzip', 'uncompressed', 'snappy']


class CompressionExperiment(ExperimentInterface):
    '''Compression experiment'''

    # Start experiment with set parameters
    def start(self, metadeploy):
        b = BenchmarkCompression()
        metadeploy.eprint('Ready to deploy!')
        return b.iterate_experiments(metadeploy)


    def stop(self, metadeploy):
        return True


    def max_nodes_needed(self):
        return max(self.nodes)