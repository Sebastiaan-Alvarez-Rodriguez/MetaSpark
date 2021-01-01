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
    for frame in reader.read_ops(node, partitions_per_node, extension, amount, kind, rb):
        # ds_arr = np.add(frame.ds_c_arr, frame.ds_i_arr)
        # spark_arr = np.add(frame.spark_c_arr, frame.spark_i_arr)
        ds_arr = frame.ds_c_arr
        spark_arr = frame.spark_c_arr
        plot_arr.append((ds_arr / 1000000000, spark_arr / 1000000000.))

        ovars = Dimension.open_vars(node, partitions_per_node, extension, amount, kind, rb)[0]
        label_arr.append(('Arrow-Spark: {}'.format(Dimension.make_id_string(frame, node, partitions_per_node, extension, amount, kind, rb)),'Spark: {}'.format(Dimension.make_id_string(frame, node, partitions_per_node, extension, amount, kind, rb)),))
        
        title_arr.append('Computation time for {} nodes'.format(frame.node))

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
        ax.plot(plot_arr[x][0], label=label_arr[x][0])
        ax.plot(plot_arr[x][1], label=label_arr[x][1])

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