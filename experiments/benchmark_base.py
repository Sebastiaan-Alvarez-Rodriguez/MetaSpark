import time

import experiments.util as eu
from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.printer import *


class BenchmarkBase(object):
    def __init__(self):
        # Cluster spawning params
        self.reserve_time = '10:00:00'
        self.config = '{}.cfg'
        self.debug_mode = False # less printing
        self.cluster_deploy_mode = DeployMode.LOCAL
        self.no_interact = True # We want to run batch jobs, so no user interaction

        # Data deployment params
        self.data_jar = 'arrow-spark-benchmark-1.0-all.jar'
        self.random_data = False
        self.data_args = '-np {} -p {}/ --format {} -nr {} -cl {} -nc {}'
        self.data_deploy_mode = DeployMode.RAM # Must remain on RAM deploy mode for this experiment
        self.first_time_force = False # Force to generate the data, even if the directory exists, when we launch a cluster for the first time
        self.dataset_name = None
        self.num_columns = 4

        # Application deployment params
        self.jar = 'arrow-spark-benchmark-1.0-light.jar'
        self.mainclass = 'org.arrowspark.benchmark.Benchmark'
        self.args = '{} -np {} -r {} -p {}/ --format {} -nr {} -dm {}  -cl {} -cc {}'
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
        self.resultloc = fs.join(fs.abspath(), '..', 'base_res')

        # Experiment params
        self.partitions_per_nodes = [16] # One DAS5 node has 16 physical, 32 logical cores, we use an X amount of partitions per physical core
        self.nodes = [8]
        self.amount = 600000000
        self.amount_multipliers = [64] # makes number of rows this factor larger using symlinks
        self.extensions = ['pq']
        self.compressions = ['uncompressed']
        self.compute_columns = [4]
        self.kinds = ['df']
        self.rbs = [8192]

        self.runs = 31 # We run our selfementation and the Spark baseline selfementation X times
        self.retries = 2 # If our application dies X times, we stop trying and move on
        self.appl_sleeptime = 30 # Sleep X seconds between checks
        self.appl_dead_after_tries = 14 # If results have not changed between X block checks, we think the application has died


    # Perform an experiment with given parameters
    def do_experiment(self, metadeploy, reservation, node, extension, compression, compute_column, amount_multiplier, kind, rb, partitions_per_node):
        # We try to do the experiment a given number of times
        # Each time we crash, we compute how many results we are missing 
        # and try to do that many runs in the next iteration.
        status = True

        # Generate data to /local/<name>/num_columns, jarfile adds /amount/partitions/(rnd)extension_<compression>/
        if self.dataset_name:
            deploypath = fs.join(loc.get_node_data_dir(DeployMode.LOCAL), self.dataset_name, self.num_columns)
        else:
            deploypath = fs.join(loc.get_node_data_dir(DeployMode.LOCAL), self.num_columns)

        data_runargs = self.data_args.format(node*partitions_per_node, deploypath, extension, self.amount, compression, self.num_columns)
        extension_filepath = extension
        if self.first_time_force:
            data_runargs += ' -gf'
        if self.random_data:
            data_runargs += ' -gr'
            extension_filepath = 'rnd'+extension_filepath
        if extension == 'pq' and compression != 'uncompressed':
            extension_filepath = '{}_{}'.format(extension_filepath, compression)

        generate_cmd = '$JAVA_HOME/bin/java -jar ~/{} {} > /dev/null 2>&1'.format(self.data_jar, data_runargs)
        if not eu.deploy_data_fast(metadeploy, reservation, generate_cmd, self.dataset_name, node, partitions_per_node, extension, self.amount, amount_multiplier, self.num_columns, extension_filepath):
            exit(1)

        for extra_arg in ['--arrow-only', '--spark-only']:
            # Place output files on ../baseres/num_columns/compute_column/node/partitions_per_node/(rnd)extension_<compression>/amount*multiplier/kind/file.res_<identifier>
            outputloc = fs.join(self.resultloc, self.num_columns, compute_column, node, partitions_per_node, extension_filepath, self.amount*amount_multiplier, kind, '{}.{}.{}.{}.{}.res_{}'.format(node, extension, self.amount*amount_multiplier, kind, rb, extra_arg[2]))
            runs_to_perform = self.runs

            for x in range(self.retries):
                partitions = node*partitions_per_node

                runargs = self.args.format(kind, partitions, runs_to_perform, deploypath, extension, self.amount, amount_multiplier, compression, compute_column)
                runargs += ' {}'.format(extra_arg)
                if self.random_data:
                    runargs += ' -gr'
                final_submit_opts = self.submit_opts.format(outputloc)+' --conf \'spark.sql.parquet.columnarReaderBatchSize={}\''.format(rb)
                if not metadeploy.deploy_application(reservation, self.jar, self.mainclass, runargs, self.extra_jars, final_submit_opts, self.no_results_dir):
                    printe('!! Fatal error when trying to deploy application !! ({})'.format(outputloc))
                    status = False
                    break
                if self.flamegraph_time != None and not metadeploy.deploy_flamegraph(reservation, self.flamegraph_time, only_master=self.flamegraph_only_master, only_worker=self.flamegraph_only_worker):
                    printw('Could not deploy all flamegraphs')
                if metadeploy.block(eu.blockfunc, args=(metadeploy, outputloc, runs_to_perform), sleeptime=self.appl_sleeptime, dead_after_retries=self.appl_dead_after_tries):
                    break # We are done!
                else: # Something bad happened. Do remaining runs in next iteration
                    finished_runs = eu.check_num_results(outputloc)
                    runs_to_perform -= (finished_runs-1) #-1 because we need a 'cold buffer' run before going on
                    outputloc += '_'+str(x)
                if x == self.retries-1:
                    metadeploy.eprint('\n\n!!!FATALITY!!! for {}\n\n'.format(outputloc))
                    status = False
                    break
        return status


    def iterate_experiments(self, metadeploy):
        for partitions_per_node in self.partitions_per_nodes:
            for rb in self.rbs:
                for extension in self.extensions:
                    for compression in self.compressions:
                        for compute_column in self.compute_columns:
                            for node in self.nodes:
                                reservation = metadeploy.cluster_start(self.reserve_time, self.config.format(node), self.debug_mode, str(self.cluster_deploy_mode), self.no_interact)
                                if not reservation:
                                    printe('!! Fatal error when trying to start cluster !! ({})'.format(outputloc))
                                    return False
                                time.sleep(5) #Give slaves time to connect to master
                                for amount_multiplier in self.amount_multipliers:
                                    for kind in self.kinds:
                                        metadeploy.clean_junk(reservation, deploy_mode=self.cluster_deploy_mode)
                                        do_experiment(self, metadeploy, reservation, node, extension, compression, compute_column, amount_multiplier, kind, rb, partitions_per_node)
                                metadeploy.cluster_stop(reservation)