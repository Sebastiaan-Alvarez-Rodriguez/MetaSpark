import os
import subprocess
import time

from experiments.interface import ExperimentInterface
from dynamic.metadeploy import MetaDeployState
from remote.util.deploymode import DeployMode
# We suggest all experiments which print anything to the console
# to use below import statement. This forces in-order printing. 
import util.fs as fs
import util.location as loc
from util.printer import *


def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return BaseExperiment

class BaseExperiment(ExperimentInterface):
    '''All experimentation we want to do, in one short script'''

    # Provides resultdirectory location
    def resultloc(self):
        return fs.join(fs.abspath(), '../', 'findres')

    def init_params(self):
        # Cluster spawning params
        self.reserve_time = '02:00:00'
        self.config = '{}.cfg'
        self.debug_mode = False # less printing
        self.cluster_deploy_mode = DeployMode.LOCAL
        self.no_interact = True # We want to run batch jobs, so no user interaction

        # Data deployment params
        self.data_jar = 'arrow-spark-benchmark-1.0-all.jar'
        self.random_data = False
        self.data_args = '-np {} -rb {} -p {}/ --format {} -nr {}'
        self.data_deploy_mode = DeployMode.RAM
        self.first_time_force = False # Force to generate the data, even if the directory exists, when we launch a cluster for the first time
        self.skip_if_exists = True

        # Application deployment params
        self.jar = 'arrow-spark-benchmark-1.0-light.jar'
        self.mainclass = 'org.sebastiaan.experiments.Experimenter'
        self.args = '{} -np {} -r {} -rb {} -p {}/ --format {} -nr {}'
        self.extra_jars = None
        shared_submit_ops = '-Dio.netty.allocator.directMemoryCacheAlignment=64 -Dfile={0} -XX:+FlightRecorder'
        self.submit_opts = '\
        --conf \'spark.executor.extraJavaOptions={0}\' \
        --conf \'spark.driver.extraJavaOptions={0}\' \
        --driver-memory 60G \
        --executor-memory 60G'.format(shared_submit_ops)
        self.no_results_dir = True
        self.flamegraph_time = '1m'

        # Experiment params
        self.partitions_per_nodes = [16] # One DAS5 node has 16 physical, 32 logical cores, we use an X amount of partitions per physical core
        self.nodes = [4]
        self.amounts = [600000000]
        self.extensions = ['pq']
        self.kinds = ['df']
        self.rbs = [1024*256]

        self.runs = 5 # We run our implementation and the Spark baseline implementation X times
        self.retries = 2 # If our application dies X times, we stop trying and move on
        self.appl_sleeptime = 40 # Sleep X seconds between checks
        self.appl_dead_after_tries = 3 # If results have not changed between X block checks, we think the application has died

    # Returns number of results in given output log location
    def check_runs(self, outputloc):
        command = 'wc -l {}'.format(outputloc)
        try:
            return int(subprocess.check_output(command, shell=True).decode('utf-8').split(' ')[0])
        except Exception as e:
            return 0

    # Function to coordinate blocking
    # We block while we have not lines_needed lines in the output log
    def are_we_done_yet(self, metadeploy, outputloc, lines_needed):
        numlines = self.check_runs(outputloc)
        metadeploy.eprint('PING: Have {}/{} results for loc {}'.format(numlines, lines_needed, outputloc))
        if numlines >= lines_needed:
            return MetaDeployState.COMPLETE, numlines
        else:
            return MetaDeployState.BUSY, numlines

    # Deploys all data from /local to /dev/shm. 
    # If the data is not in /local, we deploy data from NFS mount to /local first and then proceed.
    # This is indeed a seemingly unnecessary step. However, next time we run this function,
    # we can deploy data straight from /local, which should be lots faster than copying from NFS mount again.
    def deploy_data_fast(self, metadeploy, node, extension, amount, kind, rb, partitions_per_node, force_generate=False):
        command = 'rm -rf {} > /dev/null 2>&1'.format(loc.get_node_raw_ram_dir())
        if not metadeploy.deploy_nonspark_application(command):
            # There were some files that we could not remove, possibly permission issues.
            # Just go on
            printw('Could not "{}" on all nodes!'.format(command))

        # Generate to /local
        deploypath = loc.get_node_data_dir(DeployMode.LOCAL)
        runargs = self.data_args.format(node*partitions_per_node, rb, deploypath, extension, amount)
        if force_generate:
            runargs += ' -gf'
        command = '$JAVA_HOME/bin/java -jar ~/{} {} > /dev/null 2>&1'.format(self.data_jar, runargs)
        printw('Generate command: '+str(command))
        if not metadeploy.deploy_nonspark_application(command):
            printe('!! Fatal error when trying to deploy data using application...')
            return False

        # make directories on remotes
        command = 'mkdir -p {}'.format(fs.join(loc.get_node_data_dir(DeployMode.RAM), amount, node*partitions_per_node))
        printw('mkdir command: '+str(command))
        if not metadeploy.deploy_nonspark_application(command):
            printe('!! Fatal error when trying to mkdir on RAM (command used: "{}")'.format(command))
            return False

        # copy data to RAM directory
        frompath = fs.join(loc.get_node_data_dir(DeployMode.LOCAL), amount, node*partitions_per_node, extension)
        topath = fs.join(loc.get_node_data_dir(DeployMode.RAM), amount, node*partitions_per_node)
        command = 'cp -r {} {}'.format(frompath, topath)
        printw('cp command: '+str(command))
        if not metadeploy.deploy_nonspark_application(command):
            printe('!! Fatal error when trying to cp data to RAM (command used: "{}")'.format(command))
            return False
        prints('Deployed data successfully to "{}"'.format(topath))
        return True


    # Perform an experiment with given parameters
    def do_experiment(self, metadeploy, node, extension, amount, kind, rb, partitions_per_node):
        # We try to do the experiment a given number of times
        # Each time we crash, we compute how many results we are missing 
        # and try to do that many runs in the next iteration.
        status = True
        for extra_arg in ['--arrow-only', '--spark-only']:
            outputloc = fs.join(self.resultloc(), node, partitions_per_node, extension, amount, kind, '{}.{}.{}.{}.{}.res_{}'.format(node, extension, amount, kind, rb, extra_arg[2]))
            runs_to_perform = self.runs

            for x in range(self.retries):
                partitions = node*partitions_per_node
                if self.data_deploy_mode != DeployMode.STANDARD:
                    self.deploy_data_fast(metadeploy, node, extension, amount, kind, rb, partitions_per_node, force_generate=(x==0 and self.first_time_force))

                runargs = self.args.format(kind, partitions, runs_to_perform, rb, loc.get_node_data_dir(self.data_deploy_mode), extension, amount)
                runargs += ' {}'.format(extra_arg)
                if not metadeploy.deploy_application(self.jar, self.mainclass, runargs, self.extra_jars, self.submit_opts.format(outputloc), self.no_results_dir, self.flamegraph_time):
                    printe('!! Fatal error when trying to deploy application !! ({})'.format(outputloc))
                    status = False
                    break

                if metadeploy.block(self.are_we_done_yet, args=(metadeploy, outputloc, runs_to_perform), sleeptime=self.appl_sleeptime, dead_after_retries=self.appl_dead_after_tries):
                    break # We are done!
                else: # Something bad happened. Do remaining runs in next iteration
                    finished_runs = self.check_runs(outputloc)
                    runs_to_perform -= (finished_runs-1) #-1 because we need a 'cold buffer' run before going on
                    outputloc += '_'+str(x)
                if x == self.retries-1:
                    metadeploy.eprint('\n\n!!!FATALITY!!! for {}\n\n'.format(outputloc))
                    print('\n\n!!!FATALITY!!! for {}\n\n'.format(outputloc))
                    status = False
                    break

    # Start experiment with set parameters
    def start(self, metadeploy):
        self.init_params()
        metadeploy.eprint('Ready to deploy!')
        
        for partitions_per_node in self.partitions_per_nodes:
            for rb in self.rbs:
                for extension in self.extensions:
                    for node in self.nodes:
                        metadeploy.cluster_stop()
                        if not metadeploy.cluster_start(self.reserve_time, self.config.format(node), self.debug_mode, str(self.cluster_deploy_mode), self.no_interact):
                            printe('!! Fatal error when trying to start cluster !! ({})'.format(outputloc))
                            return False
                        time.sleep(5) #Give slaves time to connect to master
                        # metadeploy.clean_junk(datadir=loc.get_node_data_dir(self.data_deploy_mode)) # Clean entire datadir
                        for amount in self.amounts:
                            for kind in self.kinds:
                                # metadeploy.clean_junk() # Clean regular junk right after application
                                self.do_experiment(metadeploy, node, extension, amount, kind, rb, partitions_per_node)
                                # metadeploy.clean_junk(datadir=fs.join(loc.get_node_data_dir(self.data_deploy_mode), amount, (node*partitions_per_node), extension)) # Clean junk, including datadir

    def stop(self, metadeploy):
        for rb in self.rbs:
            for extension in self.extensions:
                for node in self.nodes:
                    for amount in self.amounts:
                        for kind in self.kinds:
                            outputloc = fs.join(self.resultloc(), node, extension, amount, kind, '{}.{}.{}.{}.{}.res'.format(node, extension, amount, kind, rb))
                            print('Results for "extension: {}, node: {}, amount: {}, kind: {}"'.format(extension, node, amount, kind))
                            cmd = 'cat {} | tail -n 6'.format(outputloc)
                            try:
                                os.command(cmd)
                            except Exception as e:
                                pass
                                # printe('No results found on path "{}"'.format(outputloc))
        return True #metadeploy.clean_junk()