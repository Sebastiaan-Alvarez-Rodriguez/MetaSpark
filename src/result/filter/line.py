import matplotlib.pyplot as plt
import numpy as np
import scipy.stats as sc

from result.util.reader import Reader
import result.util.storer as storer
import util.fs as fs
import util.location as loc

# Plots execution time, using provided filters
def stats(resultdir, partition, extension, amount, kind, rb, large, no_show, store_fig, filetype, skip_internal):
    path = fs.join(loc.get_metaspark_results_dir(), resultdir)

    # print('When looking at the 32, pq, 10000, 20480*8 category:')
    reader = Reader(path)
    for frame in reader.read_ops(partition, extension, amount, kind, rb):
        if large:
            fontsize = 24
            font = {
                'family' : 'DejaVu Sans',
                'weight' : 'bold',
                'size'   : fontsize
            }
            plt.rc('font', **font)

        fig, ax = plt.subplots()
        
        ds_arr = np.add(frame.ds_c_arr, frame.ds_i_arr)
        spark_arr = np.add(frame.spark_c_arr, frame.spark_i_arr)
        # ds_arr.sort()
        # spark_arr.sort()
        
        ax.plot(ds_arr / 1000000000, label='Dataset')
        ax.plot(spark_arr / 1000000000, label='Spark')
        ax.set(xlabel='Execution number (increasing in time)', ylabel='Time (s)', title='Total execution time for {} partitions'.format(frame.partition))
        if large:
            ax.legend(loc='right', fontsize=18, frameon=False)
        else:
            ax.legend(loc='right', frameon=False)
    
        if large:
            fig.set_size_inches(10, 8)

        fig.tight_layout()

        if store_fig:
           storer.store(resultdir, 'line', filetype, plt)

        if large:
            plt.rcdefaults()

        if not no_show:
            plt.show()