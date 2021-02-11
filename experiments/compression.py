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
        self.nodes = [4, 8, 16, 32]

        # Application deployment params
        shared_submit_ops = '-Dio.netty.allocator.directMemoryCacheAlignment=64 -Dfile={0} -XX:+FlightRecorder'
        self.submit_opts = '\
        --conf \'spark.executor.extraJavaOptions={0}\' \
        --conf \'spark.driver.extraJavaOptions={0}\' \
        --conf \'spark.memory.offHeap.size={1}\' \
        --conf \'spark.memory.offHeap.enabled={2}\' \
        --driver-memory 60G \
        --executor-memory 60G'.format(shared_submit_ops, 0 if self.offheap_memory == None else self.offheap_memory, 'false' if self.offheap_memory == None else 'true')
        self.no_results_dir = True
        self.flamegraph_time = '3000s'
        self.flamegraph_only_master = False
        self.flamegraph_only_worker = True

        # Experiment params
        self.compressions = ['gzip', 'uncompressed', 'snappy']
        self.appl_sleeptime = 5 # Sleep X seconds between checks
        self.appl_dead_after_tries = 120 # If results have not changed between X block checks, we think the application has died


class CompressionExperiment(ExperimentInterface):
    '''Compression experiment'''

    # Start experiment with set parameters
    def start(self, metadeploy):
        b = BenchmarkCompression()
        metadeploy.eprint('Ready to deploy!')
        b.iterate_experiments(metadeploy)


    def stop(self, metadeploy):
        return True