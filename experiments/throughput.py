import time

import remote.util.balancer as balancer
from experiments.interface import ExperimentInterface
import util.fs as fs
import util.location as loc 
from util.printer import *


def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return ThroughputExperiment

class ThroughputExperiment(ExperimentInterface):
    '''
    Experiment measuring throughput performance of saturated servers
    as server nodes get killed
    '''
    def num_servers(self):
        '''Get amount of server nodes to allocate'''
        return 5

    def num_clients(self):
        '''get amount of client nodes to allocate'''
        return 256

    def servers_use_infiniband(self):
        '''True if servers must communicate with eachother over infiniband, False otherwise'''
        return True

    def clients_use_infiniband(self):
        '''True if clients must communicate with servers over infinband, False otherwise'''
        return False

    def servers_core_affinity(self):
        '''Amount of server processes which may be mapped on the same physical node'''
        return 1

    def clients_core_affinity(self):
        '''Amount of client processes which may be mapped on the same physical node'''
        return 8

    def server_periodic_clean(self):
        '''Period in seconds for servers to clean their crawlspaces. 0 means no cleaning'''
        return 8

    def get_read_ratios(self):
        return ['0', '25', '50', '75', '90', '100']    

    def get_run_command(self, metaspark, i):
        return 'java -Dlog4j.configuration=file:"{}" "-Dprops={}" -jar {} {} {} {} {}'.format(
        fs.join(loc.get_client_cfg_dir(), 'log4j.properties'),
        'ERROR, CONSOLE',
        fs.join(loc.get_metaspark_dep_dir(), 'throughput_client', 'zookeeper-client.jar'),
        balancer.balance_simple(metaspark.hosts, metaspark.gid),
        metaspark.gid, 
        fs.join(loc.get_node_log_dir(), '{}.log'.format(metaspark.gid)), 
        self.get_read_ratios()[i])

    # Constructs symlinks in zookeeper-client directory, so classpath resolves to required jars 
    def _prepare_classpath_symlinks(self):
        locations = [
            fs.join(loc.get_build_lib_dir(), 'log4j-1.2.15.jar'),
            fs.join(loc.get_build_dir(), 'zookeeper-3.3.0.jar'),
        ]

        fs.mkdir(fs.join(loc.get_metaspark_dep_dir(), 'throughput_client'), exist_ok=True)
        for path in locations:
            dst = fs.join(loc.get_metaspark_dep_dir(), 'throughput_client', fs.basename(path))
            if not fs.exists(dst):
                fs.ln(path, dst)


    def pre_experiment(self, metaspark):
        self._prepare_classpath_symlinks()
        '''Execution before experiment starts. Executed on the remote once.'''
        metaspark.register['time'] = 5
        print('''
Attention! Due to the large amount of clients constantly reading and writing data,
snapshots and logs grow quickly to terrabyte-scale. Due to storage quota,
we periodically remove old logs and snapshots.
Happy experimenting!
''')

    def get_client_run_command(self, metaspark):
        '''Get client run command, executed in All client nodes'''
        return self.get_run_command(metaspark, 0)


    def experiment_client(self, metaspark):
        '''Execution occuring on ALL client nodes'''
        run_time = metaspark.register['time']
        time.sleep(run_time) #Client remains active for a while
        nr_ratios = len(self.get_read_ratios())
        for ratio in range(1, nr_ratios):
            metaspark.executor.cmd = self.get_run_command(metaspark, ratio)
            metaspark.executor.reboot()
            time.sleep(run_time)

        metaspark.executor.stop()

    def experiment_server(self, metaspark):
        '''Execution occuring on ALL server nodes'''
        sleep_time = 2 * metaspark.register['time'] * len(self.get_read_ratios())
        time.sleep(sleep_time)

    def post_experiment(self, metaspark):
        print('Experiments are done')