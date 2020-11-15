import numpy as np

from result.util.reader import Reader
import util.fs as fs
import util.location as loc


'''
Prints basic stats for our experiment results, using filters.
Filters work as follows:
For any given filter type {partition, extension, amount, kind, rb}:
1. If there is no filter set (value == None),
    then we match all available items
2. Otherwise, we match all items that are in the filter list
'''
def stats(resultdir, partition, extension, amount, kind, rb):
    path = fs.join(loc.get_metaspark_results_dir(), resultdir)

    # print('When looking at the 32, pq, 10000, 20480*8 category:')
    reader = Reader(path)
    for frame in reader.read_ops(partition, extension, amount, kind, rb):
        print('''
--- partition {}, extension {}, amount {}, kind {}, rb {}:
Dataset
Total time: {:.3f}s  ({:.3f}s data, {:.3f}s compute)
Avg time:   {:.3f}s  ({:.3f}s data, {:.3f}s compute)
stddev:     {:.3f}s  ({:.3f}s data, {:.3f}s compute)
Incorrect answers: {}
Spark
Total time: {:.3f}s  ({:.3f}s data, {:.3f}s compute)
Avg time:   {:.3f}s  ({:.3f}s data, {:.3f}s compute)
stddev:     {:.3f}s  ({:.3f}s data, {:.3f}s compute)
Incorrect answers: {}'''.format(
    frame.partition, frame.extension, frame.amount, frame.kind, frame.rb,
    frame.ds_total_time, frame.ds_d_time, frame.ds_c_time,
    frame.ds_total_avgtime, frame.ds_d_avgtime, frame.ds_c_avgtime,
    np.std(np.add(frame.ds_d_arr, frame.ds_c_arr))/1000000000, np.std(frame.ds_d_arr)/1000000000, np.std(frame.ds_c_arr)/1000000000,
    frame.ds_incorrect,
    frame.spark_total_time, frame.spark_d_time, frame.spark_c_time,
    frame.spark_total_avgtime, frame.spark_d_avgtime, np.average(frame.spark_c_avgtime),
    np.std(np.add(frame.spark_d_arr, frame.spark_c_arr))/1000000000, np.std(frame.spark_d_arr)/1000000000, np.std(frame.spark_c_arr)/1000000000,
    frame.spark_incorrect
    ))