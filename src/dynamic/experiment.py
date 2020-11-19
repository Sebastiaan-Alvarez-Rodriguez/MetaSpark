import inspect
import sys

from dynamic.metadeploy import MetaDeploy
from experiments.interface import ExperimentInterface
import util.fs as fs
import util.importer as imp
import util.location as loc
import util.ui as ui


class Experiment(object):
    '''
    Object to handle communication with user-defined experiment interface
    Almost all attributes are lazy, so the dynamic code is used minimally.
    '''
    def __init__(self, location, clazz):
        self.location = location
        self.instance = clazz()
        self._metaDeploy = MetaDeploy()

    @property
    def metaDeploy(self):
        return self._metaDeploy


    def start(self):
        return self.instance.start(self._metaDeploy)


    def stop(self):
        return self.instance.stop(self._metaDeploy)


# Standalone function to get an experiment instance
def get_experiments():
    candidates = []
    for item in fs.ls(loc.get_metaspark_experiments_dir(), full_paths=True, only_files=True):
        if item.endswith(fs.join(fs.sep(), 'interface.py')) or not item.endswith('.py'):
            continue
        try:
            module = imp.import_full_path(item)
            candidates.append((item, module.get_experiment(),))
        except AttributeError:
            printw('Item had no get_experiment(): {}. Skipping for now...'.format(item))

    if len(candidates) == 0:
        raise RuntimeError('Could not find a subclass of "ExperimentInterface" in directory {}. Make a ".py" file there, with a class extending "ExperimentInterface". See the example implementation for more details.'.format(loc.get_metaspark_experiments_dir()))
    elif len(candidates) == 1:
        return [Experiment(candidates[0][0], candidates[0][1])]
    else:
        idcs = ui.ask_pick_multiple('Multiple suitable experiments found. Please pick experiments:', [x[0] for x in candidates])
        return [Experiment((candidates[x])[0], (candidates[x])[1]) for idx, x in enumerate(idcs)]