from enum import Enum
import subprocess
import time

from util.executor import Executor

class State(Enum):
    '''Possible states for experiments'''
    UNALLOCATED = 0
    ALLOCATED = 1
    RUNNING = 2
    FINISHED = 3


class ExperimentComposition(object):
    '''Trivial composition object to hold experiment with its state'''
    def __init__(self, experiment, state=State.UNALLOCATED, cluster=None):
        if cluster != None:
            assert state == ALLOCATED
        self.experiment = experiment
        self.state = state
        self.cluster = cluster


    def set_allocated(cluster):
        self.cluster = cluster
        self.state = State.ALLOCATED


    def max_nodes_needed(self):
        return self.experiment.max_nodes_needed()



class ClusterRegistry(object):
    '''Trivial registry object to hold clusters with free-state'''
    def __init__(self, clusters):
        self.clusters = dict()
        for x in clusters:
            self.clusters[x] = True

    def get_free():
        return (key for key, value in self.clusters if value)

    def get_allocated():
        return (key for key, value in self.clusters if not value)

    def mark(cluster, now_free):
        assert cluster in self.clusters
        self.clusters[cluster] = now_free


class Allocator(object):
    '''
    Object to handle allocations and maintain minimal state.
    Current problem: We check if experiments are done using a poke.
    The poke only returns clusters with a reservation in R-state.
    If an experiment is switching clusters, might be it is in PD-state.
    It might even occur that we poke just when the system switches,
    so we would see no reservation.
    In those cases, the system thinks experiment is done, and launches a new one incorrectly.

    Only solution: Poke e.g. 3 times, and only when 3 pokes in a row show no reservation, then
    we register it as a finished experiment.
    '''
    def __init__(self, experiments, clusters, allocator_func, check_time_seconds=120):
        # Sort experiments based on max nodes needed, most needed first
        self.experiments = [ExperimentComposition(x) for x in sorted(experiments, key=lambda x: x.max_nodes_needed(), reverse=True)]
        self.cluster_registry = ClusterRegistry(clusters)
        self.cluster_known_capable = ClusterRegistry(clusters)
        
        max_available = max(x.total_nodes for x in clusters)
        if len(self.experiments) > 0 and self.experiments[0].max_nodes_needed() > max_available:
            max_cluster = sorted(clusters, key=lambda x: x.total_nodes, reverse=True)[0]
            printe('Can never allocate largest experiment at "{}": Experiment needs more nodes ({}) than we have in any cluster (max {} in cluster {})'.format(experiment.location, experiment.max_nodes_needed(), max_available, max_cluster.ssh_key_name))
            return False

        self.allocator_func = allocator_func
        self.check_time_seconds = check_time_seconds


    def allocate(self):
        # Sort clusters based on available nodes, most available first
        available_clusters = self.cluster_registry.get_free()
        clusters_sorted = [x for x in sorted(zip(self.get_available_nodes(available_clusters), available_clusters), key=lambda x: x[0], reverse=True)]
        
        allocated = []
        for experiment in self.experiments:
            if x.state != State.UNALLOCATED:
                continue
            if len(clusters_sorted) == 0:
                return allocated, []
            nodes_available, cluster = clusters_sorted[0] # pick cluster with largest available room
            self.cluster_registry.mark(cluster, now_free=False)
            experiment.set_allocated(cluster)
            allocated.append((cluster, experiment)) #cluster fits!
            del clusters_sorted[0] # remove cluster from list, cannot host multiple experiments at once
        return allocated


    def num_clusters_available(self):
        return len(self.cluster_registry.get_free())


    def finished(self):
        return any((x for x in self.experiments if x.state != State.FINISHED))


    # Pokes given clusters in parallel. Returns the indices of the clusters busy running an experiment
    def distributed_poke(self, clusters):
        executors = [Executor('ssh {} "python3 deploy check_active"'.format(x.ssh_key_name)) for x in clusters]
        busy_clusters = [idx for idx, val in enumerate(Executor.wait_all(executors, stop_on_error=False, return_returncodes=True)) if val == 1]
        return busy_clusters


    # Main allocation function. 
    def execute(self):
        while not self.finished():
            if self.num_clusters_available() > 0:
                allocated = self.allocate()
                for cluster, experiment in allocated:
                    self.allocator_func(cluster, experiment)

            watched_experiments = [idx for idx, x in enumerate(self.experiments) if x.state == State.ALLOCATED or x.state == State.RUNNING]
            indices = self.distributed_poke((self.experiments[idx].cluster for idx in watched_experiments))
            for idx in indices:
                if self.experiments[idx].state == State.ALLOCATED:
                    self.experiments[idx].state = State.RUNNING
                    self.cluster_known_capable.mark(self.experiments[idx].cluster, True) # We know this cluster is capable of running
            for idx in set(range(len(self.experiments))) - set(indices):
                if self.experiments[idx].state == State.RUNNING:
                    self.experiments[idx].state = State.FINISHED
                    self.cluster_registry.mark(self.experiments[idx], now_free=True)

            # Sleep for a bit, until it is time to check again
            time.sleep(self.check_time_seconds)


    def get_available_nodes(self, clusters=None):
        if clusters == None:
            clusters = self.cluster_registry.get_free()
        executors = [Executor('ssh {} "python3 deploy numnodes"'.format(x.ssh_key_name)) for x in clusters]
        used_nodes = Executor.wait_all(executors, stop_on_error=False, return_returncodes=True)
        return [x-y for x,y in zip([z.total_nodes for z in clusters], used_nodes)]


    def get_available_nodes_for(self, cluster):
        command = 'ssh {} "python3 deploy numnodes"'.format(cluster.ssh_key_name)
        used_nodes = subprocess.call(command, shell=True)
        return cluster.total_nodes - used_nodes