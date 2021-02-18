# This file contains a fast log reader for increasing log numbers
import numpy as np
import itertools

import util.fs as fs
from util.printer import *
from result.util.dimension import Dimension

def _match(val, nullable_arr=None):
    return True if nullable_arr == None or len(nullable_arr) == 0 else str(val) in nullable_arr

def filename_to_rb(filename):
    return int(filename.split('.')[-2])

class Reader(object):
    '''
    Object to read data from a path.
    Directory structure is assumed to be
    <num_cols>/<compute_cols>/<node>/<partitions_per_node>/<extension>/<compression>/<amount>/<kind>/<rb>/
    
    Only reads files ending on '.res'.
    Reader is ignorant of all other files and directories
    '''
    def __init__(self, path_start):
        if not fs.isdir(path_start):
            raise RuntimeError('Cannot read data from path "{}"'.format(path_start))
        self.path = path_start

    def _filter_apply(self, paths, filter_, name='Item', sort=False, only_dirs=False):
        l = list()
        for path in paths:
            for item in sorted(fs.ls(path, only_dirs=only_dirs, full_paths=True), key=lambda x: int(fs.basename(x))) if sort else fs.ls(path, only_dirs=only_dirs, full_paths=True):
                to_filter = fs.basename(item)
                if not _match(to_filter, filter_):
                    printw('{}={} did not match filter={}'.format(name, to_filter, filter_))
                    continue
                l.append(item)
        return l

    def _filter_files(self, num_cols=None, compute_cols=None, node=None, partitions_per_node=None, extension=None, compression=None, amount=None, kind=None, rb=None):
        self.files = [self.path]

        names = ['num_cols', 'compute_cols', 'node', 'partitions_per_node', 'extension', 'compression', 'amount', 'kind', 'rb', '']
        for idx, filter_ in enumerate([num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb, None]):
            self.files = self._filter_apply(self.files, filter_, name=names[idx], sort=Dimension.open_var_numeric(names[idx]), only_dirs=idx!=len(names)-1)

        self.files.sort()
        print('Matched {} files'.format(len(self.files)))

    # Converts a path to a resultfile to a list of identifying parameters
    def _path_to_identifiers(self, path_to_file):
        return fs.dirname(path_to_file).split(fs.sep())[-9:]


    # Lazily read and return data using a filter
    # Optionally filter the first 2 measurements, which are uncached
    # Data is provided as it0, ct0, a0, it1, ct1, a1
    def read_ops(self, num_cols=None, compute_cols=None, node=None, partitions_per_node=None, extension=None, compression=None, amount=None, kind=None, rb=None, skip_leading=3):
        self._filter_files(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb)

        if len(self.files) > 0:
            if self.files[0].endswith('_a'): # We deal with the 'new' system, with separate arrow and spark result files
                for file_a, file_b in zip(self.files[0::2], self.files[1::2]):
                    identifiers_a = self._path_to_identifiers(file_a)
                    identifiers_b = self._path_to_identifiers(file_b)
                    if identifiers_a != identifiers_b:
                        raise RuntimeError('Error: Found that identifiers are not equivalent of files in same directory')
                    with open(file_a, 'r') as f_a:
                        frame_a = Frame(*identifiers_a, 'arrow', f_a.readlines(), skip_leading)
                    with open(file_b, 'r') as f_b:
                        frame_b = Frame(*identifiers_a, 'spark', f_b.readlines(), skip_leading)
                    if frame_a.empty and frame_b.empty:
                        printw('Skipping an empty pair of frames')
                        continue
                    yield (frame_a, frame_b)

            else: # We deal with the old system, with 1 file containing alternating arrow and spark results
                print('Old system detected')
                for file in self.files:
                    with open(file, 'r') as f:
                        identifiers = fs.dirname(file).split(fs.sep())[-5:] + [filename_to_rb(file)]
                        lines = f.readlines()
                        frame_a = Frame(*identifiers, 'arrow', lines[::2], skip_leading)
                        frame_b = Frame(*identifiers, 'spark', lines[1::2], skip_leading)  
                        if frame_a.empty and frame_b.empty:
                            printw('Skipping an empty pair of frames')
                            continue
                        yield (frame_a, frame_b)

    @property
    def num_files(self):
        return len(self.files) if self.files else 0


class Frame(object):
    '''Frames hold data in numpy arrays, with identifiers'''

    # Initialize a new frame.
    # If skip_initial is set, we skip the first 2 entries of lines.
    # This is useful, because the first 2 entries are without any cached values 
    def __init__(self, num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb, tag, lines, skip_leading=3):
        if skip_leading > 0:
            print('Note: Skipping first {} result(s), use "--skip-leading <num>" to change'.format(skip_leading))
            lines = lines[skip_leading:]
        itimes = [int(x.split(',', 1)[0]) for idx, x in enumerate(lines)]
        ctimes = [int(x.split(',', 1)[1]) for idx, x in enumerate(lines)]

        # Identifiers
        self.num_cols = num_cols
        self.compute_cols = compute_cols
        self.node = int(node)
        self.partitions_per_node = int(partitions_per_node)
        self.extension = extension
        self.compression = compression
        self.amount = int(amount)
        self.kind = kind
        self.rb = int(rb)
        self.tag = tag

        self.i_arr = np.array(itimes)
        self.c_arr = np.array(ctimes)



    def __len__(self):
        return self.size

    @property
    def size(self):
        return len(self.i_arr)

    @property
    def empty(self):
        return self.size == 0
    

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