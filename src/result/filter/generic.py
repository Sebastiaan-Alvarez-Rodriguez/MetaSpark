import numpy as np

from result.util.reader import Reader
import util.fs as fs
import util.location as loc


'''
Prints basic stats for our experiment results, using filters.
Filters work as follows:
For any given filter type {node, extension, amount, kind, rb}:
1. If there is no filter set (value == None),
    then we match all available items
2. Otherwise, we match all items that are in the filter list
'''
def stats(resultdir, node, partitions_per_node, extension, amount, kind, rb, skip_initial):
    path = fs.join(loc.get_metaspark_results_dir(), resultdir)

    reader = Reader(path)
    for frame in reader.read_ops(node, partitions_per_node, extension, amount, kind, rb, skip_initial):
        print('''
--- {} nodes, {} partitions per node, extension '{}', amount {}, kind {}, rb {} ({} measurements):
Arrow-Spark ({} measurements)
Total time: {:.3f}s  ({:.3f}s init, {:.3f}s compute, {:.3f}s nodeing)
Avg time:   {:.3f}s  ({:.3f}s init, {:.3f}s compute, {:.3f}s nodeing)
stddev:     {:.3f}s  ({:.3f}s init, {:.3f}s compute, {:.3f}s nodeing)
Spark       ({} measurements)
Total time: {:.3f}s  ({:.3f}s init, {:.3f}s compute)
Avg time:   {:.3f}s  ({:.3f}s init, {:.3f}s compute)
stddev:     {:.3f}s  ({:.3f}s init, {:.3f}s compute)
'''.format(
    frame.node, frame.partitions_per_node, frame.extension, frame.amount, frame.kind, frame.rb, frame.size,
    frame.ds_size,
    frame.ds_total_time, frame.ds_i_time, frame.ds_c_time, frame.ds_p_time,
    frame.ds_total_avgtime, frame.ds_i_avgtime, frame.ds_c_avgtime, frame.ds_p_avgtime,
    np.std(np.add(frame.ds_i_arr, frame.ds_c_arr))/1000000000, np.std(frame.ds_i_arr)/1000000000, np.std(frame.ds_c_arr)/1000000000, np.std(frame.ds_p_arr)/1000000000,
    frame.spark_size,
    frame.spark_total_time, frame.spark_i_time, frame.spark_c_time,
    frame.spark_total_avgtime, frame.spark_i_avgtime, np.average(frame.spark_c_avgtime),
    np.std(np.add(frame.spark_i_arr, frame.spark_c_arr))/1000000000, np.std(frame.spark_i_arr)/1000000000, np.std(frame.spark_c_arr)/1000000000
    ))