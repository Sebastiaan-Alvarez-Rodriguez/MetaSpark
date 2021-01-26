# This file contains a fast log reader for increasing log numbers
import numpy as np
import itertools

import util.fs as fs
from util.printer import *

def _match(val, nullable_arr=None):
    return True if nullable_arr == None or len(nullable_arr) == 0 else str(val) in nullable_arr

def filename_to_rb(filename):
    return int(filename.split('.')[-2])

class Reader(object):
    '''
    Object to read data from a path.
    Directory structure is assumed to be
    <node>/<extension>/<amount>/<kind>/<rb>/
    
    Only reads files ending on '.res'.
    Reader is ignorant of all other files and directories
    '''
    def __init__(self, path_start):
        if not fs.isdir(path_start):
            raise RuntimeError('Cannot read data from path "{}"'.format(path_start))
        self.path = path_start

    def _filter_files(self, node=None, partitions_per_node=None, extension=None, amount=None, kind=None, rb=None):
        self.files = []
        for dnode in sorted(fs.ls(self.path, only_dirs=True), key=lambda x: int(x)):
            if not _match(dnode, node):
                printw('Node={} did not match filter={}'.format(dnode, node))
                continue
            for dpartitions_per_node in sorted(fs.ls(fs.join(self.path, dnode), only_dirs=True), key=lambda x: int(x)):
                if not _match(dpartitions_per_node, partitions_per_node):
                    printw('PartitionsPerNode={} did not match filter={}'.format(dpartitions_per_node, partitions_per_node))
                    continue
                for dextension in fs.ls(fs.join(self.path, dnode, dpartitions_per_node), only_dirs=True):
                    if not _match(dextension, extension):
                        printw('Extension={} did not match filter={}'.format(dextension, extension))
                        continue
                    for damount in sorted(fs.ls(fs.join(self.path, dnode, dpartitions_per_node, dextension), only_dirs=True), key=lambda x: int(x)):
                        if not _match(damount, amount):
                            printw('Amount={} did not match filter={}'.format(damount, amount))
                            continue
                        for dkind in fs.ls(fs.join(self.path, dnode, dpartitions_per_node, dextension, damount), only_dirs=True):
                            if not _match(dkind, kind):
                                printw('Kind={} did not match filter={}'.format(dkind, kind))
                                continue
                            tmp_matched = sorted([x for x in fs.ls(fs.join(self.path, dnode, dpartitions_per_node, dextension, damount, dkind), only_files=True, full_paths=True) if x.endswith('.res') or x.endswith('.res_a') or x.endswith('.res_s')], key=lambda x: filename_to_rb(x))
                            for outfile in tmp_matched:
                                frb = filename_to_rb(outfile)
                                if not _match(frb, rb):
                                    printw('Buffersize={} did not match filter={}'.format(frb, rb))
                                    continue
                                self.files.append(outfile)
        self.files.sort()
        print('Matched {} files'.format(len(self.files)))

    # Lazily read and return data using a filter
    # Optionally filter the first 2 measurements, which are uncached
    # Data is provided as it0, ct0, a0, it1, ct1, a1
    def read_ops(self, node=None, partitions_per_node=None, extension=None, amount=None, kind=None, rb=None, skip_initial=True):
        self._filter_files(node, partitions_per_node, extension, amount, kind, rb)

        if len(self.files) > 0:
            if self.files[0].endswith('_a'): # We deal with the 'new' system, with separate arrow and spark result files
                print('New system detected')
                for file_a, file_b in zip(self.files[0::2], self.files[1::2]):

                    identifiers_a = fs.dirname(file_a).split(fs.sep())[-5:] + [filename_to_rb(file_a)]
                    identifiers_b = fs.dirname(file_b).split(fs.sep())[-5:] + [filename_to_rb(file_b)]
                    if identifiers_a != identifiers_b:
                        raise RuntimeError('Error: Found that identifiers are not equivalent of files in same directory')
                    with open(file_a, 'r') as f_a:
                        frame_a = Frame(*identifiers_a, 'arrow', f_a.readlines(), skip_initial)
                    with open(file_b, 'r') as f_b:
                        frame_b = Frame(*identifiers_a, 'spark', f_b.readlines(), skip_initial)
                    yield (frame_a, frame_b)

            else: # We deal with the old system, with 1 file containing alternating arrow and spark results
                for file in self.files:
                    with open(file, 'r') as f:
                        identifiers = fs.dirname(file).split(fs.sep())[-5:] + [filename_to_rb(file)]
                        lines = f.readlines()
                        lines_a = lines[::2]
                        lines_b = lines[1::2] 
                        yield (Frame(*identifiers, 'arrow', lines_a, skip_initial), Frame(*identifiers, 'spark', lines_b, skip_initial))

    @property
    def num_files(self):
        return len(self.files) if self.files else 0


class Frame(object):
    '''Frames hold data in numpy arrays, with identifiers'''

    # Initialize a new frame.
    # If skip_initial is set, we skip the first 2 entries of lines.
    # This is useful, because the first 2 entries are without any cached values 
    def __init__(self, node, partitions_per_node, extension, amount, kind, rb, tag, lines, skip_initial=True):
        if skip_initial:
            lines = lines[1:]
        itimes = [int(x.split(',', 1)[0]) for idx, x in enumerate(lines)]
        ctimes = [int(x.split(',', 1)[1]) for idx, x in enumerate(lines)]

        # Identifiers
        self.node = int(node)
        self.partitions_per_node = int(partitions_per_node)
        self.extension = extension
        self.amount = int(amount)
        self.kind = kind
        self.rb = int(rb)
        self.tag = tag

        self.i_arr = np.array(itimes)
        self.c_arr = np.array(ctimes)



    def __len__(self):
        return self.size_

    @property
    def size(self):
        return len(self.i_arr)


    @property
    def i_time(self):
        return float(np.sum(self.i_arr)) / 1000000000
    
    @property
    def c_time(self):
        return float(np.sum(self.c_arr)) / 1000000000
    
    @property
    def total_time(self):
        return self.i_time+self.c_time
    
    @property
    def i_avgtime(self):
        return np.average(self.i_arr) / 1000000000
        
    @property
    def c_avgtime(self):
        return np.average(self.c_arr) / 1000000000
    
    @property
    def total_avgtime(self):
        return self.i_avgtime+self.c_avgtime