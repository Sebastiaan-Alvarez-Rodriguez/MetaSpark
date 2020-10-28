import abc

def get_experiment():
    '''Implement this function in your experiment, make it return your experiment class'''
    return FaultToleranceExperiment

class ExperimentInterface(metaclass=abc.ABCMeta):
    '''
    This interface provides hooks, which get triggered on specific moments in execution.
    Experiments must be defined inside this interface.
    MetaSpark dynamically imports and executes it.

    Check <root dir>/metaspark/experiments/examples/example_simple.py 
    for an example implementation.
    Also, check <root dir>/metaspark/dynamic/metaspark.py
    to find out how metaspark works and what it provides.
    '''
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'num_servers') and callable(subclass.num_servers) and 
                hasattr(subclass, 'num_clients') and callable(subclass.num_clients) and 
                hasattr(subclass, 'servers_use_infiniband') and callable(subclass.servers_use_infiniband) and 
                hasattr(subclass, 'clients_use_infiniband') and callable(subclass.clients_use_infiniband) and 
                hasattr(subclass, 'servers_core_affinity') and callable(subclass.servers_core_affinity) and 
                hasattr(subclass, 'clients_core_affinity') and callable(subclass.clients_core_affinity) and 
                hasattr(subclass, 'server_periodic_clean') and callable(subclass.server_periodic_clean) and
                hasattr(subclass, 'pre_experiment') and callable(subclass.pre_experiment) and 
                hasattr(subclass, 'get_client_run_command') and callable(subclass.get_client_run_command) and 
                hasattr(subclass, 'experiment_client') and callable(subclass.experiment_client) and 
                hasattr(subclass, 'experiment_server') and callable(subclass.experiment_server) and 
                hasattr(subclass, 'post_experiment') and callable(subclass.post_experiment) or NotImplemented)


    @abc.abstractmethod
    def num_servers(self):
        '''Get amount of server nodes to allocate'''
        raise NotImplementedError

    @abc.abstractmethod
    def num_clients(self):
        '''get amount of client nodes to allocate'''
        raise NotImplementedError

    @abc.abstractmethod
    def servers_use_infiniband(self):
        '''True if servers must communicate with eachother over infiniband, False otherwise'''
        raise NotImplementedError

    @abc.abstractmethod
    def clients_use_infiniband(self):
        '''True if clients must communicate with servers over infinband, False otherwise'''
        raise NotImplementedError

    @abc.abstractmethod
    def servers_core_affinity(self):
        '''Amount of server processes which may be mapped on the same physical node'''
        raise NotImplementedError

    @abc.abstractmethod
    def clients_core_affinity(self):
        '''Amount of client processes which may be mapped on the same physical node'''
        raise NotImplementedError

    @abc.abstractmethod
    def server_periodic_clean(self):
        '''Period in seconds for servers to clean their crawlspaces. 0 means no cleaning'''
        raise NotImplementedError


    @abc.abstractmethod
    def pre_experiment(self, metaspark):
        '''Execution before experiment starts. Executed on the remote once.'''
        raise NotImplementedError

    @abc.abstractmethod
    def get_client_run_command(self, metaspark):
        '''Get client run command, executed in All client nodes'''
        raise NotImplementedError

    @abc.abstractmethod
    def experiment_client(self, metaspark):
        '''Execution occuring on ALL client nodes'''
        raise NotImplementedError

    @abc.abstractmethod
    def experiment_server(self, metaspark):
        '''Execution occuring on ALL server nodes'''
        raise NotImplementedError


    @abc.abstractmethod
    def post_experiment(self, metaspark):
        '''Execution after experiment finishes. Executed no the remote once.'''
        raise NotImplementedError