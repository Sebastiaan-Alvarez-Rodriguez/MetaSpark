import abc

def get_experiment():
    '''Implement this function in your experiment, make it return your experiment class'''
    raise NotImplementedError

class ExperimentInterface(metaclass=abc.ABCMeta):
    '''
    This interface provides hooks, which get triggered on specific moments in deployment execution.
    It is your job to call the deployment control functions,
    which are available in <root dir>/src/dynamic/metadeploy.py

    Check <root dir>/MetaSpark/experiments/examples/example_simple.py 
    for an example implementation.
    '''
    # @classmethod
    # def __subclasshook__(cls, subclass):
    #     return (hasattr(subclass, 'start') and callable(subclass.num_servers) and 
    #             hasattr(subclass, 'finish') and callable(subclass.num_clients) and 
    #             hasattr(subclass, 'servers_use_infiniband') and callable(subclass.servers_use_infiniband) and 
    #             hasattr(subclass, 'clients_use_infiniband') and callable(subclass.clients_use_infiniband) and 
    #             hasattr(subclass, 'servers_core_affinity') and callable(subclass.servers_core_affinity) and 
    #             hasattr(subclass, 'clients_core_affinity') and callable(subclass.clients_core_affinity) and 
    #             hasattr(subclass, 'server_periodic_clean') and callable(subclass.server_periodic_clean) and
    #             hasattr(subclass, 'pre_experiment') and callable(subclass.pre_experiment) and 
    #             hasattr(subclass, 'get_client_run_command') and callable(subclass.get_client_run_command) and 
    #             hasattr(subclass, 'experiment_client') and callable(subclass.experiment_client) and 
    #             hasattr(subclass, 'experiment_server') and callable(subclass.experiment_server) and 
    #             hasattr(subclass, 'post_experiment') and callable(subclass.post_experiment) or NotImplemented)

    def start(self, metadeploy):
        raise NotImplementedError

    def stop(self, metadeploy):
        raise NotImplementedError 

    def max_nodes_needed(self):
        raise NotImplementedError