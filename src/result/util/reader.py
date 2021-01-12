# This file contains a fast log reader for increasing log numbers
import numpy as np

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
                # printw('Node={} did not match filter={}'.format(dnode, node))
                continue
            for dpartitions_per_node in sorted(fs.ls(fs.join(self.path, dnode), only_dirs=True), key=lambda x: int(x)):
                if not _match(dpartitions_per_node, partitions_per_node):
                    # printw('PartitionsPerNode={} did not match filter={}'.format(dpartitions_per_node, partitions_per_node))
                    continue
                for dextension in fs.ls(fs.join(self.path, dnode, dpartitions_per_node), only_dirs=True):
                    if not _match(dextension, extension):
                        # printw('Extension={} did not match filter={}'.format(dextension, extension))
                        continue
                    for damount in sorted(fs.ls(fs.join(self.path, dnode, dpartitions_per_node, dextension), only_dirs=True), key=lambda x: int(x)):
                        if not _match(damount, amount):
                            # printw('Amount={} did not match filter={}'.format(damount, amount))
                            continue
                        for dkind in fs.ls(fs.join(self.path, dnode, dpartitions_per_node, dextension, damount), only_dirs=True):
                            if not _match(dkind, kind):
                                # printw('Kind={} did not match filter={}'.format(dkind, kind))
                                continue
                            for outfile in sorted([x for x in fs.ls(fs.join(self.path, dnode, dpartitions_per_node, dextension, damount, dkind), only_files=True, full_paths=True) if x.endswith('.res')], key=lambda x: filename_to_rb(x)):
                                frb = filename_to_rb(outfile)
                                if not _match(frb, rb):
                                    continue
                                self.files.append(outfile)
        print('Matched {} files'.format(len(self.files)))

    # Lazily read and return data using a filter
    # Optionally filter the first 2 measurements, which are uncached
    # Data is provided as it0, ct0, a0, it1, ct1, a1
    def read_ops(self, node=None, partitions_per_node=None, extension=None, amount=None, kind=None, rb=None, skip_initial=True):
        self._filter_files(node, partitions_per_node, extension, amount, kind, rb)
        for file in self.files:
            with open(file, 'r') as f:
                identifiers = fs.dirname(file).split(fs.sep())[-5:] + [filename_to_rb(file)]
                yield Frame(*identifiers, f.readlines(), skip_initial)

    @property
    def num_files(self):
        return len(self.files) if self.files else 0


class Frame(object):
    '''Frames hold data in numpy arrays, with identifiers'''

    # Initialize a new frame.
    # If skip_initial is set, we skip the first 2 entries of lines.
    # This is useful, because the first 2 entries are without any cached values 
    def __init__(self, node, partitions_per_node, extension, amount, kind, rb, lines, skip_initial=True):
        if len(lines) % 2 != 0:
            lines = lines[:-1]
        if skip_initial:
            lines = lines[2:]
        ptimes = [int(x.split(',', 1)[0]) for x in lines[::2]]
        itimes = [int(x.split(',', 2)[1-(idx % 2)]) for idx, x in enumerate(lines)]
        ctimes = [int(x.split(',', 2)[2-(idx % 2)]) for idx, x in enumerate(lines)]

        # Identifiers
        self.node = int(node)
        self.partitions_per_node = int(partitions_per_node)
        self.extension = extension
        self.amount = int(amount)
        self.kind = kind
        self.rb = int(rb)

        self.ds_p_arr = np.array(ptimes[::2])

        self.ds_i_arr = np.array(itimes[::2])
        self.spark_i_arr = np.array(itimes[1::2])

        self.ds_c_arr = np.array(ctimes[::2])
        self.spark_c_arr = np.array(ctimes[1::2])


    def __len__(self):
        return self.size

    @property
    def size(self):
        return self.ds_size + self.spark_size

    @property
    def ds_size(self):
        return len(self.ds_i_arr)

    @property
    def spark_size(self):
        return len(self.spark_i_arr)

        
    # Dataset total times spent in locations
    @property
    def ds_p_time(self):
        return float(np.sum(self.ds_p_arr)) / 1000000000

    @property
    def ds_i_time(self):
        return float(np.sum(self.ds_i_arr)) / 1000000000
    
    @property
    def ds_c_time(self):
        return float(np.sum(self.ds_c_arr)) / 1000000000
    
    @property
    def ds_total_time(self):
        return self.ds_i_time+self.ds_c_time
    
    # Spark total times spent in locations
    @property
    def spark_i_time(self):
        return float(np.sum(self.spark_i_arr)) / 1000000000
    
    @property
    def spark_c_time(self):
        return float(np.sum(self.spark_c_arr)) / 1000000000
    
    @property
    def spark_total_time(self):
        return self.spark_i_time+self.spark_c_time
    
    # Dataset average times spent in locations
    @property
    def ds_p_avgtime(self):
        return np.average(self.ds_p_arr) / 1000000000
     
    @property
    def ds_i_avgtime(self):
        return np.average(self.ds_i_arr) / 1000000000
        
    @property
    def ds_c_avgtime(self):
        return np.average(self.ds_c_arr) / 1000000000
    
    @property
    def ds_total_avgtime(self):
        return self.ds_i_avgtime+self.ds_c_avgtime
    
    # Spark average times spent in locations
    @property
    def spark_i_avgtime(self):
        return np.average(self.spark_i_arr) / 1000000000
        
    @property
    def spark_c_avgtime(self):
        return np.average(self.spark_c_arr) / 1000000000
    
    @property
    def spark_total_avgtime(self):
        return self.spark_i_avgtime+self.spark_c_avgtime