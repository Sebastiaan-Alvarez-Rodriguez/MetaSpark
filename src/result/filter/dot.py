import matplotlib.pyplot as plt
import numpy as np
import scipy.stats as sc

from result.util.dimension import Dimension
from result.util.reader import Reader
import result.util.storer as storer
import util.fs as fs
import util.location as loc

# Plots execution time, using provided filters
def stats(resultdir, node, partitions_per_node, extension, amount, kind, rb, large, no_show, store_fig, filetype, skip_internal):
    path = fs.join(loc.get_metaspark_results_dir(), resultdir)

    reader = Reader(path)
    plot_arr = []
    label_arr = []
    title_arr = []
    for frame_arrow, frame_spark in reader.read_ops(node, partitions_per_node, extension, amount, kind, rb):
        # ds_arr = np.add(frame_arrow.c_arr, frame_arrow.i_arr)
        # spark_arr = np.add(frame_spark.c_arr, frame_spark.i_arr)
        arrow_arr = frame_arrow.c_arr
        spark_arr = frame_spark.c_arr
        plot_arr.append((arrow_arr / 1000000000, spark_arr / 1000000000))

        ovars = Dimension.open_vars(node, partitions_per_node, extension, amount, kind, rb)[0]
        label_arr.append(('Arrow-Spark: {}'.format(Dimension.make_id_string(frame_arrow, node, partitions_per_node, extension, amount, kind, rb)),'Spark: {}'.format(Dimension.make_id_string(frame_spark, node, partitions_per_node, extension, amount, kind, rb)),))
        
        title_arr.append('Computation time for {} nodes'.format(frame_arrow.node))

    if large:
        fontsize = 24
        font = {
            'family' : 'DejaVu Sans',
            'weight' : 'bold',
            'size'   : fontsize
        }
        plt.rc('font', **font)

    fig, ax = plt.subplots()
    for x in range(len(plot_arr)):
        ax.plot(plot_arr[x][0], 'o', label=label_arr[x][0])
        ax.plot(plot_arr[x][1], 'o', label=label_arr[x][1])

    ax.set(xlabel='Execution number', ylabel='Time (s)', title='Computation times')

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