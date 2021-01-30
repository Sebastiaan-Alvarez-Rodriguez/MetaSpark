import numpy as np
import itertools

from result.util.reader import Reader
import util.fs as fs
import util.location as loc

def print_for_frame(frame):
    print('''
--- {} nodes, {} partitions per node, extension '{}', compression {}, amount {}, kind {}, rb {} ({} measurements):
{}       ({} measurements)
Total time: {:.3f}s  ({:.3f}s init, {:.3f}s compute)
Avg time:   {:.3f}s  ({:.3f}s init, {:.3f}s compute)
stddev:     {:.3f}s  ({:.3f}s init, {:.3f}s compute)
'''.format(
    frame.node, frame.partitions_per_node, frame.extension, frame.compression, frame.amount, frame.kind, frame.rb, frame.size,
    frame.tag, frame.size,
    frame.total_time, frame.i_time, frame.c_time,
    frame.total_avgtime, frame.i_avgtime, np.average(frame.c_avgtime),
    np.std(np.add(frame.i_arr, frame.c_arr))/1000000000, np.std(frame.i_arr)/1000000000, np.std(frame.c_arr)/1000000000
    ))

'''
Prints basic stats for our experiment results, using filters.
Filters work as follows:
For any given filter type {node, extension, compression, amount, kind, rb}:
1. If there is no filter set (value == None),
    then we match all available items
2. Otherwise, we match all items that are in the filter list
'''
def stats(resultdir, node, partitions_per_node, extension, compression, amount, kind, rb, skip_initial):
    path = fs.join(loc.get_metaspark_results_dir(), resultdir)

    reader = Reader(path)
    for frame_a, frame_b in reader.read_ops(node, partitions_per_node, extension, compression, amount, kind, rb, skip_initial):
        print_for_frame(frame_a)
        print_for_frame(frame_b)