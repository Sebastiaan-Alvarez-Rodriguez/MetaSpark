import random
import time

from experiments.interface import ExperimentInterface
from dynamic.metadeploy import MetaDeployState

# We suggest all experiments which print anything to the console
# to use below import statement. This forces in-order printing. 
import util.fs as fs
from util.printer import *


def get_experiment():
    '''Pass your defined experiment class in this function so MetaZoo can find it'''
    return ExampleExperiment

class ExampleExperiment(ExperimentInterface):
    '''
    A most useful experiment.
    '''

    def start(self, metadeploy):
        # Cluster deployment params
        config_filename = 'test.cfg' # Use cluster setup as described in <project root>/conf/cluster/test.cfg
        debug_mode = False # less printing
        fast = True # Use a fast cluster deployment strategy
        no_interact = True # We want to run batch jobs, so no user interaction
        # Application deployment params
        time_to_reserve = '15:00' # Just reserve the nodes for 15 minutes
        no_results_dir = True # We will not store any results, so we do not need a results dir for each run
        # Data deployment params
        skip_if_exists = True # Skip copying files if they already exist on the node

        state_ok = True # Used to return whether we had any errors in our runs
        for x in range(10):
            # It is up to you to provide some jar file.
            # We chose the standard spark examples jarfile.
            # If you are going to run this example, please make sure
            # spark-examples_2.12-3.0.1.jar is available in <project root>/jars/
            jarfile = 'spark-examples_2.12-3.0.1.jar' 
            mainclass = 'org.apache.spark.examples.SparkPi' # We will run the PiSpark example
            # We tell PiSpark how many partitions it should have
            # As you can see, this changes each run
            args = str(x*10)
            # We don't need extra jars to get PiSpark to work
            extra_jars = None
            # We don't need any special spark-submit options for PiSpark
            submit_opts = None
            
            print('Starting up a cluster...')
            state_ok &= metadeploy.cluster_start(time_to_reserve, config_filename, debug_mode, fast, no_interact)
            print('Cleaning junk data...')
            state_ok &= metadeploy.clean_junk()
            print('Deploying data to allocated nodes...')

            # We can copy data collected in <project root>/data/ to local drives of all nodes.
            # We call this feature 'Data deployment'. 
            # Note: deploy_data prepends <project root>/data/ to given paths
            # Note: Each time you start a cluster, you should deploy your data that your application needs again!
            #       The reason for that is simple: We may have been given new nodes, that do not contain the data yet.
            # 
            # An example call is given below
            # metadeploy.deploy_data(['MYFILE.txt', 'ADIRPATH/DIR/'], skip_if_exists)
            print('Deploying application...')
            state_ok &= metadeploy.deploy_application(jarfile, mainclass, args, extra_jars, submit_opts, no_results_dir)
            
            print('Blocking until we are done...')
            # We block until the am_i_done_yet function returns MetaDeployState.COMPLETE
            # We query this function once every 4 seconds
            # If the function returns the same second returnvalue (randval) 5 times, we stop blocking
            if metadeploy.block(self.am_i_done_yet, args=(0.30,), sleeptime=4, dead_after_retries=5):
                print('Excellent news: We have finished execution iteration {}!'.format(x))
            else:
                print('Terrible news: We failed execution of iteration {}!'.format(x))
                state_ok = False
            state_ok &= metadeploy.cluster_stop()
            # We stop and reboot the cluster in the loop
            # If we don't do that, and execution takes > 15:00,
            # then we find out we cannot deploy applications/data
            # on dead reservations in following iterations.
        return state_ok

    def am_i_done_yet(self, some_param):
        randval = random.random()
        if randval < some_param:
            return MetaDeployState.COMPLETE, randval
        else:
            print('Still doing some more work...')
            return MetaDeployState.BUSY, randval

    def stop(self, metadeploy):
        return metadeploy.clean_junk()