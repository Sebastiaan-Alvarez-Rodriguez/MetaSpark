# This file contains a fast kill_log reader for identify the Leader Kill events

import util.fs as fs

class Identifier(object):
    '''
    Object to read faulttolerance data from a path for different runs
    Expects a path containing the log_kill files
    Identifier is ignorant of all other files and directories
    '''
    def __init__(self, path):
        if not fs.isfile(path):
            raise RuntimeError('Cannot read kills logs from path "{}"'.format(path))
        self._path = path

    # Returns which events were Leader Kills
    def identify_leaders(self):
        with open(self._path, 'r') as file:
            return [line.rstrip().split(',')[2] == 'True' for line in file.readlines()]
        