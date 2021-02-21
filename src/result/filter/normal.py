import matplotlib.pyplot as plt
import numpy as np
import scipy.stats as sc

from result.util.reader import Reader
from result.util.dimension import Dimension
import result.util.storer as storer
import util.fs as fs
import util.location as loc

# Plots normal distributions in one figure, using provided filters
def stats(resultdir, num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb, large, no_show, store_fig, filetype, skip_leading):
    path = fs.join(loc.get_metaspark_results_dir(), resultdir)

    # print('When looking at the 32, pq, 10000, 20480*8 category:')
    reader = Reader(path)
    for frame_arrow, frame_spark in reader.read_ops(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb, skip_leading,):
        if large:
            fontsize = 28
            font = {
                'family' : 'DejaVu Sans',
                'size'   : fontsize
            }
            plt.rc('font', **font)

        fig, ax = plt.subplots(2)
        # Multiple plots: https://matplotlib.org/devdocs/gallery/subplots_axes_and_figures/subplots_demo.html
        ds_arr = np.add(frame_arrow.c_arr, frame_arrow.i_arr) / 1000000000
        spark_arr = np.add(frame_spark.c_arr, frame_spark.i_arr) / 1000000000
        ds_arr.sort()
        spark_arr.sort()

        ds_pdf = sc.norm.pdf(ds_arr, np.average(ds_arr), np.std(ds_arr))
        spark_pdf = sc.norm.pdf(spark_arr, np.average(spark_arr), np.std(spark_arr))
        ax[0].plot(ds_arr, ds_pdf, label='Arrow-Spark')
        ax[1].plot(spark_arr, spark_pdf, label='Spark')

        ax[0].set(xlabel='Time (s) for '+Dimension.make_id_string(frame_arrow, node, partitions_per_node, extension, compression, amount, kind, rb), ylabel='Probability density', title='Total execution time for Arrow-Spark')
        ax[1].set(xlabel='Time (s)', ylabel='Probability density', title='Total execution time for Spark')
        
        if large:
            fig.set_size_inches(16, 8)

        fig.tight_layout()

        if store_fig:
           storer.store(resultdir, 'normal', filetype, plt)

        if large:
            plt.rcdefaults()

        if not no_show:
            plt.show()