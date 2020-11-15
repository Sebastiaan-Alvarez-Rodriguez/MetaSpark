import matplotlib.pyplot as plt
import numpy as np
from itertools import zip_longest

from result.util.reader import Reader 
import result.util.storer as storer
import util.fs as fs
import util.location as loc


# We process basic stats for our experiment results here
def stats(resultdir, large, no_show, store_fig, filetype):
    seconds = 5     #Amount of seconds client runs each run, for each read ratio
    path = fs.join(loc.get_metaspark_results_dir(), resultdir)

    if large:
        fontsize = 24
        font = {
            'family' : 'DejaVu Sans',
            'weight' : 'bold',
            'size'   : fontsize
        }
        plt.rc('font', **font)

    #
    # partitions = [16, 8, 4, 32]
    # extensions = ['pq', 'csv']
    # amounts = [10000, 100000, 1000000, 10000000, 100000000]
    # kinds = ['rdd', 'df', 'df_sql', 'ds']
    # rbs = [20480, 20480*2, 20480*4, 20480*8, 20480*16]
    # 

    print('When looking at the 32, pq, 10000, rdd category:')
    reader = Reader(path)
    correct_ans = 10000*(10000-1)/2
    for frame in reader.read_ops(partition=32, extension='pq', amount=10000, kind='rdd'):
        ds_d_arr = np.array([x[0] for x in frame.data])
        ds_c_arr = np.array([x[1] for x in frame.data])
        ds_a_arr = np.array([x[2] for x in frame.data])
        spark_d_arr = np.array([x[3] for x in frame.data])
        spark_c_arr = np.array([x[4] for x in frame.data])
        spark_a_arr = np.array([x[5] for x in frame.data])

        ds_d_time = float(np.sum(ds_d_arr)) / 1000000000
        ds_c_time = float(np.sum(ds_c_arr)) / 1000000000
        ds_total_time = ds_d_time+ds_c_time
        spark_d_time = float(np.sum(spark_d_arr)) / 1000000000
        spark_c_time = float(np.sum(spark_c_arr)) / 1000000000
        spark_total_time = spark_d_time+spark_c_time

        ds_d_avgtime = np.average(ds_d_arr) / 1000000000
        ds_c_avgtime = np.average(ds_c_arr) / 1000000000
        ds_total_avgtime = ds_d_avgtime+ds_c_avgtime
        spark_d_avgtime = np.average(spark_d_arr) / 1000000000
        spark_c_avgtime = np.average(spark_c_arr) / 1000000000
        spark_total_avgtime = spark_d_avgtime+spark_c_avgtime

        ds_incorrect = len([x for x in frame.data if x[2] != correct_ans])
        spark_incorrect = len([x for x in frame.data if x[5] != correct_ans])

        print('''
--- rb {}
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
    frame.rb,
    ds_total_time, ds_d_time, ds_c_time,
    ds_total_avgtime, ds_d_avgtime, np.average(ds_c_avgtime),
    np.std(np.add(ds_d_arr, ds_c_arr))/1000000000, np.std(ds_d_arr)/1000000000, np.std(ds_c_arr)/1000000000,
    ds_incorrect,
    spark_total_time, spark_d_time, spark_c_time,
    spark_total_avgtime, spark_d_avgtime, np.average(spark_c_avgtime),
    np.std(np.add(spark_d_arr, spark_c_arr))/1000000000, np.std(spark_d_arr)/1000000000, np.std(spark_c_arr)/1000000000,
    spark_incorrect
    ))

    # reader = Reader(path)
    # for frame in reader.read_ops(partition=32, extension='pq', amount=10000, kind='rdd', rb=20480):

    if large:
        plt.rcdefaults()