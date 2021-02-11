from experiments.interface import ExperimentInterface
import experiments.util as eu
from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.printer import *

def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return DataScalabilityExperiment

class DataScalabilityExperiment(ExperimentInterface):
    '''Data scalability experiment'''

    class BenchmarkDataScalability(base.BenchmarkBase):
        def __init__(self):
            # Application deployment params
            self.resultloc = fs.join(fs.abspath(), '..', 'data_scalability_res')

            # Experiment params
            self.amount_multipliers = [4, 8, 16, 32, 64, 128] # makes number of rows this factor larger using symlinks

 
    # Start experiment with set parameters
    def start(self, metadeploy):
        b = BenchmarkDataScalability()
        metadeploy.eprint('Ready to deploy!')
        b.iterate_experiments(self, metadeploy)


    def stop(self, metadeploy):
        return True