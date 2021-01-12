import matplotlib.pyplot as plt
import numpy as np
import scipy.stats as sc

from result.util.reader import Reader
import result.util.storer as storer
import util.fs as fs
import util.location as loc

# Plots normal distributions in one figure, using provided filters
def stats(resultdir, node, partitions_per_node, extension, amount, kind, rb, large, no_show, store_fig, filetype, skip_internal):
    path = fs.join(loc.get_metaspark_results_dir(), resultdir)

    # print('When looking at the 32, pq, 10000, 20480*8 category:')
    reader = Reader(path)
    for frame in reader.read_ops(node, partitions_per_node, extension, amount, kind, rb):
        if large:
            fontsize = 24
            font = {
                'family' : 'DejaVu Sans',
                'weight' : 'bold',
                'size'   : fontsize
            }
            plt.rc('font', **font)

        fig, ax = plt.subplots(2)
        # Multiple plots: https://matplotlib.org/devdocs/gallery/subplots_axes_and_figures/subplots_demo.html
        ds_arr = np.add(frame.ds_c_arr, frame.ds_i_arr) / 1000000000
        spark_arr = np.add(frame.spark_c_arr, frame.spark_i_arr) / 1000000000
        ds_arr.sort()
        spark_arr.sort()

        ds_pdf = sc.norm.pdf(ds_arr, np.average(ds_arr), np.std(ds_arr))
        spark_pdf = sc.norm.pdf(spark_arr, np.average(spark_arr), np.std(spark_arr))
        ax[0].plot(ds_arr, ds_pdf, label='Dataset')
        ax[1].plot(spark_arr, spark_pdf, label='Spark')

        ax[0].set(xlabel='Time (s)', ylabel='Probability density', title='Total execution time for Dataset')
        ax[1].set(xlabel='Time (s)', ylabel='Probability density', title='Total execution time for Spark')
        
        if large:
            fig.set_size_inches(10, 8)

        fig.tight_layout()

        if store_fig:
           storer.store(resultdir, 'normal', filetype, plt)

        if large:
            plt.rcdefaults()

        if not no_show:
            plt.show()