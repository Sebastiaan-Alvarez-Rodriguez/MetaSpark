import matplotlib.pyplot as plt
import numpy as np
from itertools import zip_longest

from result.stats.reader import Reader 
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
    for frame in reader.read_ops(partition=32, extension='pq', amount=10000, kind='rdd'):
        time_total = float(sum(x[0]+x[3] for x in frame.data))/1000000000
        time_avg   = time_total / len(frame.data)
        print('rb {}: Total time was {} seconds, average time was {} seconds'.format(frame.rb, time_total, time_avg))
    
    if large:
        plt.rcdefaults()