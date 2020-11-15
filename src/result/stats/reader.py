# This file contains a fast log reader for increasing log numbers


import util.fs as fs
from util.printer import *

def _match(val, nullable=None):
    return True if nullable == None else val == str(nullable)

def filename_to_rb(filename):
    return int(filename.split('.')[-2])

class Reader(object):
    '''
    Object to read data from a path.
    Directory structure is assumed to be
    <partition>/<extension>/<amount>/<kind>/<rb>/
    
    Only reads files ending on '.res'.
    Reader is ignorant of all other files and directories
    '''
    def __init__(self, path_start):
        if not fs.isdir(path_start):
            raise RuntimeError('Cannot read data from path "{}"'.format(path_start))
        self.path = path_start

    def _filter_files(self, partition=None, extension=None, amount=None, kind=None, rb=None):
        self.files = []
        for dpartition in sorted(fs.ls(self.path, only_dirs=True), key=lambda x: int(x)):
            if not _match(dpartition, partition):
                printw('Partition={} did not match filter={}'.format(dpartition, partition))
                continue
            for dextension in fs.ls(fs.join(self.path, dpartition), only_dirs=True):
                if not _match(dextension, extension):
                    printw('Extension={} did not match filter={}'.format(dextension, extension))
                    continue
                for damount in sorted(fs.ls(fs.join(self.path, dpartition, dextension), only_dirs=True), key=lambda x: int(x)):
                    if not _match(damount, amount):
                        printw('Amount={} did not match filter={}'.format(damount, amount))
                        continue
                    for dkind in fs.ls(fs.join(self.path, dpartition, dextension, damount), only_dirs=True):
                        if not _match(dkind, kind):
                            printw('Kind={} did not match filter={}'.format(dkind, kind))
                            continue
                        for outfile in sorted([x for x in fs.ls(fs.join(self.path, dpartition, dextension, damount, dkind), only_files=True, full_paths=True) if x.endswith('.res')], key=lambda x: filename_to_rb(x)):
                            frb = filename_to_rb(outfile)
                            if not _match(frb, rb):
                                continue
                            self.files.append(outfile)
        print('Matched {} files'.format(len(self.files)))

    # Lazily read and return data using a filter
    # Data is provided as dt0, ct0, a0, dt1, ct1, a1
    def read_ops(self, partition=None, extension=None, amount=None, kind=None, rb=None):
        self._filter_files(partition, extension, amount, kind, rb)
        for file in self.files:
            with open(file, 'r') as f:
                identifiers = fs.dirname(file).split(fs.sep())[-4:] + [filename_to_rb(file)]
                yield Frame(*identifiers, f.readlines())
    @property
    def num_files(self):
        return len(self.files) if self.files else 0

class Frame(object):
    '''Frames are holders of data, with identifiers'''
    def __init__(self, partition, extension, amount, kind, rb, lines):
        if len(lines) % 2 != 0:
            raise RuntimeError('File "{}" has uneven amount of lines!'.format(file))
                
        self.data = [self._prepare_data(x, y) for x, y in zip(lines[::2], lines[1::2])]
        self.partition = partition
        self.extension = extension
        self.amount = amount
        self.kind = kind
        self.rb = rb

    def _prepare_data(self, line0, line1):
        datatime0, computetime0, answer0 = line0.split(', ')
        datatime1, computetime1, answer1 = line1.split(', ')
        return int(datatime0), int(computetime0), int(answer0), int(datatime1), int(computetime1), int(answer1)
