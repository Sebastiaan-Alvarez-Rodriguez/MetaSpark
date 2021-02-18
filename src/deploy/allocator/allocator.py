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
    '''
    Trivial registry object to hold clusters with state.
    Each cluster has:
    1. 'free' state. Tells if an experiment is allocated/running on a cluster.
    2. 'capable' state. Tells if we had to wait for too long for an allocated experiment to run.
    3. 'pokes' state. Gives the number of subsequent pokes that showed either
        1. no experiment is allocated/running anymore (when belonging experiment state is RUNNING)
        2. allocated experiment is not running yet (when belongin experiment state is ALLOCATED)
    '''
    def __init__(self, clusters):
        self.free = dict()
        self.capable = dict()
        self.pokes = dict()
        for x in clusters:
            self.free[x] = True
            self.capable[x] = True
            self.pokes[x] = 0

    def get_free():
        return (key for key, value in self.free if value)

    def get_allocated():
        return (key for key, value in self.free if not value)

    def mark_free(cluster, now_free):
        assert cluster in self.free
        self.free[cluster] = now_free

    def mark_capable(cluster, now_capable):
        assert cluster in self.capable
        self.capable[cluster] = now_capable

    def is_capable(cluster):
        assert cluster in self.capable
        return self.capable[cluster]

    def mark_poke(cluster):
        assert cluster in self.pokes
        self.pokes[cluster] = self.pokes[cluster]+1
        return self.pokes[cluster]

    def reset_poke(cluster):
        assert cluster in self.pokes
        self.pokes[cluster] = 0


class Allocator(object):
    '''
    Object to handle allocations and maintain minimal state.
    Current problem: Suppose we have an experiment that is just done, and an experiment scheduled
    somewhere where it will not run for another week.
    We currently only allocate unallocated experiments.
    Need to check number of pokes a system gets when in allocated state.
    Solution: If the number of pokes exceeds a treshold,
    we allocate given allocated experiment on another available cluster.
    Optionally, we can also mark the cluster as non-capable.
    During allocation-time, we would consider non-capable clusters as last alternatives to schedule on
    '''
    def __init__(self, experiments, clusters, allocator_func, check_time_seconds=120, num_pokes_to_finish=3, num_pokes_to_allocate=30):
        # Sort experiments based on max nodes needed, most needed first
        self.experiments = [ExperimentComposition(x) for x in sorted(experiments, key=lambda x: x.max_nodes_needed(), reverse=True)]
        self.cluster_registry = ClusterRegistry(clusters)

        max_available = max(x.total_nodes for x in clusters)
        if len(self.experiments) > 0 and self.experiments[0].max_nodes_needed() > max_available:
            max_cluster = sorted(clusters, key=lambda x: x.total_nodes, reverse=True)[0]
            printe('Can never allocate largest experiment at "{}": Experiment needs more nodes ({}) than we have in any cluster (max {} in cluster {})'.format(experiment.location, experiment.max_nodes_needed(), max_available, max_cluster.ssh_key_name))
            return False

        self.allocator_func = allocator_func
        self.check_time_seconds = check_time_seconds
        self.num_pokes_to_finish = num_pokes_to_finish


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
            if self.num_clusters_available() > 0 and len(x for x in self.experiments if x.state == State.UNALLOCATED or (x.state == State.ALLOCATED and not self.cluster_registry.is_capable(x.cluster))) > 0:
                allocated = self.allocate()
                for cluster, experiment in allocated:
                    self.allocator_func(cluster, experiment)

            watched_experiments = [idx for idx, x in enumerate(self.experiments) if x.state == State.ALLOCATED or x.state == State.RUNNING]
            indices = self.distributed_poke((self.experiments[idx].cluster for idx in watched_experiments))

            # Mark found running experiments/clusters
            for idx in indices:
                self.cluster_registry.reset_poke(self.experiments[idx].cluster)
                if self.experiments[idx].state == State.ALLOCATED:
                    self.experiments[idx].state = State.RUNNING

            # Mark not-running experiments/clusters
            for idx in set(range(len(self.experiments))) - set(indices):
                if self.experiments[idx].state == State.ALLOCATED:
                    # Found that experiment is not running yet. Increase poke state
                    poked = self.cluster_registry.mark_poke(self.experiments[idx].cluster)
                    if poked >= self.num_pokes_to_allocate:
                        self.cluster_registry.mark_capable(self.experiments[idx].cluster, now_capable=False)

                if self.experiments[idx].state == State.RUNNING:
                    # No longer running experiment found
                    poked = self.cluster_registry.mark_poke(self.experiments[idx].cluster)
                    if poked >= self.num_pokes_to_finish:
                        self.experiments[idx].state = State.FINISHED
                        self.cluster_registry.mark_free(self.experiments[idx].cluster, now_free=True)

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