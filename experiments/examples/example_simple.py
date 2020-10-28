import time

from experiments.interface import ExperimentInterface

# We suggest all experiments which print anything to the console
# to use below import statement. This forces in-order printing. 
from util.printer import *



def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return ExampleExperiment

class ExampleExperiment(ExperimentInterface):
    '''
    A most useful experiment.
    Check <root dir>/metaspark/experiments/example_simple/example.py 
    for an example implementation.
    Also, check <root dir>/metaspark/dynamic/metaspark.py
    to find out how metaspark variables work.
    '''

    def num_servers(self):
        '''Get amount of server nodes to allocate'''
        return 2

    def num_clients(self):
        '''get amount of client nodes to allocate'''
        return 2

    def servers_use_infiniband(self):
        '''True if servers must communicate with eachother over infiniband, False otherwise'''
        return False

    def clients_use_infiniband(self):
        '''True if clients must communicate with servers over infinband, False otherwise'''
        return False

    def servers_core_affinity(self):
        '''Amount of server processes which may be mapped on the same physical node'''
        return 1

    def clients_core_affinity(self):
        '''Amount of client processes which may be mapped on the same physical node'''
        return 1

    def server_periodic_clean(self):
        '''Period in seconds for servers to clean their crawlspaces. 0 means no cleaning'''
        return 0


    def pre_experiment(self, metaspark):
        '''Execution before experiment starts. Executed on the remote once.'''
        print('Hi there! I am executed before the experiment starts!')
        print('According to my data, we will host')

        metaspark.register['a_key'] = 'Hello World'
        metaspark.register['secret'] = 42
        if metaspark.gid == None and metaspark.lid == None:
            print('I cannot use gid and lid here yet!')


    def get_client_run_command(self, metaspark):
        '''Get client run command, executed in All client nodes'''
        return 'while :; do echo "I am a running client"; sleep 20; done'


    def experiment_client(self, metaspark):
        '''Execution occuring on ALL client nodes'''
        print('Hello from client with gid={}. I am told these hosts exist: {}'.format(metaspark.gid, metaspark.hosts))
        time.sleep(5)
        print('I (client {}:{}) slept well. Pre-experiment says "{}" with secret code {}. Goodbye!'.format(
            metaspark.gid,
            metaspark.lid,
            metaspark.register['a_key'],
            metaspark.register['secret']))


    def experiment_server(self, metaspark):
        '''Execution occuring on ALL server nodes'''
        print('I am server {}:{}, and I will try to modify the register now'.format(metaspark.gid, metaspark.lid))
        try:
            metaspark.register['secret'] = -1
        except Exception as e:
            print('Turns out I (server {}) cannot add or change or delete variables after pre_experiment. Goodbye!'.format(metaspark.id))
        time.sleep(5)
        print('I (server {}:{}) slept well. Goodbye!'.format(
            metaspark.gid,
            metaspark.lid))

    def post_experiment(self, metaspark):
        '''get amount of client nodes to allocate'''
        print('Experiments are done. Pre-experiment had this secret: {}'.format(metaspark.register['secret']))