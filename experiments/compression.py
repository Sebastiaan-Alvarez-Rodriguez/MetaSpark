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

class CompressionExperiment(ExperimentInterface):
    '''Compression experiment'''

    # Provides resultdirectory location
    def resultloc(self):
        return fs.join(fs.abspath(), '..', 'compression_res')


    def init_params(self):
        # Cluster spawning params
        self.reserve_time = '12:00:00'
        self.config = '{}.cfg'
        self.debug_mode = False # less printing
        self.cluster_deploy_mode = DeployMode.LOCAL
        self.no_interact = True # We want to run batch jobs, so no user interaction

        # Data deployment params
        self.data_jar = 'arrow-spark-benchmark-1.0-all.jar'
        self.random_data = False
        self.data_args = '-np {} -p {}/ --format {} -nr {} -cl {}'
        self.data_deploy_mode = DeployMode.RAM # Must remain on RAM deploy mode for this experiment
        self.first_time_force = False # Force to generate the data, even if the directory exists, when we launch a cluster for the first time

        # Application deployment params
        self.jar = 'arrow-spark-benchmark-1.0-light.jar'
        self.mainclass = 'org.arrowspark.benchmark.Benchmark'
        self.args = '{} -np {} -r {} -p {}/ --format {} -nr {} -dm {} -cl {}'
        self.extra_jars = None
        self.offheap_memory = None #1024*1024*1 # 1 mb of off-heap memory per JVM. Set to None to disable offheap memory
        shared_submit_ops = '-Dio.netty.allocator.directMemoryCacheAlignment=64 -Dfile={0}' # -XX:+FlightRecorder
        self.submit_opts = '\
        --conf \'spark.executor.extraJavaOptions={0}\' \
        --conf \'spark.driver.extraJavaOptions={0}\' \
        --conf \'spark.memory.offHeap.size={1}\' \
        --conf \'spark.memory.offHeap.enabled={2}\' \
        --driver-memory 60G \
        --executor-memory 60G'.format(shared_submit_ops, 0 if self.offheap_memory == None else self.offheap_memory, 'false' if self.offheap_memory == None else 'true')
        self.no_results_dir = True
        self.flamegraph_time = None

        # Experiment params
        self.partitions_per_nodes = [16] # One DAS5 node has 16 physical, 32 logical cores, we use an X amount of partitions per physical core
        self.nodes = [8]
        self.amount = 600000000
        self.amount_multipliers = [4, 8, 16, 32, 64] # makes number of rows this factor larger using symlinks
        self.extensions = ['pq']
        self.compressions = ['uncompressed', 'snappy', 'gzip'] # 'uncompressed', 'lz0', 'brotli', 'lz4', 'zstd'
        self.kinds = ['df']
        self.rbs = [8192]

        self.runs = 31 # We run our implementation and the Spark baseline implementation X times
        self.retries = 2 # If our application dies X times, we stop trying and move on
        self.appl_sleeptime = 30 # Sleep X seconds between checks
        self.appl_dead_after_tries = 20 # If results have not changed between X block checks, we think the application has died


    # Start experiment with set parameters
    def start(self, metadeploy):
        self.init_params()
        metadeploy.eprint('Ready to deploy!')
        base.iterate_experiments(self, metadeploy)


    def stop(self, metadeploy):
        return True