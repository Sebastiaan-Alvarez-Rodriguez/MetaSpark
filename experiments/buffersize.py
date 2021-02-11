from experiments.interface import ExperimentInterface
import experiments.benchmark_base as base
import experiments.util as eu
from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.printer import *

def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return BufferSizeExperiment

class BufferSizeExperiment(ExperimentInterface):
    '''Buffer size varying experiment.'''

        class BenchmarkBuffersize(base.BenchmarkBase):
            def __init__(self):
                # Cluster spawning params
                self.reserve_time = '12:00:00'

                # Application deployment params
                self.resultloc = fs.join(fs.abspath(), '..', 'buffersize_res')

                # Experiment params
                self.rbs = [1024, 2048, 4096, 8192, 16384, 32768]


    # Start experiment with set parameters
    def start(self, metadeploy):
        b = BenchmarkBuffersize()
        metadeploy.eprint('Ready to deploy!')
        b.iterate_experiments(self, metadeploy)


    def stop(self, metadeploy):
        return True