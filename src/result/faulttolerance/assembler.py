# This file contains an assembler to read logs from multiple runs

from itertools import zip_longest

from result.faulttolerance.reader import Reader
from result.faulttolerance.identifier import Identifier
import util.fs as fs

def get_kill_logs(path):
    return fs.join(path, 'experiment_logs', 'kills.log')



class Assembler(object):
    '''
    Object to read faulttolerance data from a path for different runs
    Expects a path directories containing files '0.log', '1.log', ....
    Each directory should also contain experiment_logs/kills.log
    Assembler is ignorant of all other files and directories
    '''
    def __init__(self, path):
        if not fs.isdir(path):
            raise RuntimeError('Cannot read logs from path "{}"'.format(path))
        if not fs.isdir(fs.join(path, 0)):
            raise RuntimeError('Did not find experiments in given path "{}"').format(path)
        self._path = path

    # Lazily read and return operations per run as they are needed
    def read_ops(self):
        for run in fs.ls(self._path, only_dirs=True, full_paths=True):
            reader = Reader(run)
            yield [sum(x) for x in zip_longest(*[reader.read_ops(x) for x in range(reader.num_files)], fillvalue=0)]

    # Returns which events were Leader Kills
    def identify_leaders(self):
        return (Identifier(get_kill_logs(run)).identify_leaders() for run in fs.ls(self._path, only_dirs=True, full_paths=True))
