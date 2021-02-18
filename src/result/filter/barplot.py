import matplotlib.pyplot as plt
import numpy as np

from result.util.dimension import Dimension
from result.util.reader import Reader
import result.util.storer as storer
import util.fs as fs
import util.location as loc

# https://matplotlib.org/3.1.1/gallery/lines_bars_and_markers/bar_stacked.html

# Plots execution time, using provided filters
def stats(resultdir, num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb, large, no_show, store_fig, filetype, skip_leading):
    path = fs.join(loc.get_metaspark_results_dir(), resultdir)

    if Dimension.num_open_vars(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb) > 1:
        print('Too many open variables: {}'.format(', '.join(Dimension.open_vars(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb))))
        return

    if large:
        print('Going large!')
        fontsize = 24
        font = {
            'family' : 'DejaVu Sans',
            'weight' : 'bold',
            'size'   : fontsize
        }
        plt.rc('font', **font)

    ovar = Dimension.open_vars(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb)[0]

    reader = Reader(path)
    fig, ax = plt.subplots()

    plot_items = []
    for frame_arrow, frame_spark in reader.read_ops(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb, skip_leading, skip_leading):
        # BAR1loc
        i0 = frame_arrow.i_avgtime
        c0 = frame_arrow.c_avgtime
        x0 = getattr(frame_arrow, str(ovar))
        # BAR2loc
        i1 = frame_spark.i_avgtime
        c1 = frame_spark.c_avgtime
        x1 = getattr(frame_spark, str(ovar))
        plot_items.append((i0,i1,c0,c1,x0,x1))

    if len(plot_items) == 0:
        print('No results to plot. Exiting now...')
        return

    plot_items.sort(key=lambda item: int(item[4]) if ovar.is_numeric else item[4]) # Will sort on x0. x0==x1==ovar, the open variable
    i_arr0 = [x[0] for x in plot_items]
    i_arr1 = [x[1] for x in plot_items]
    c_arr0 = [x[2] for x in plot_items]
    c_arr1 = [x[3] for x in plot_items]
    xticks0 =[ovar.val_to_ticks(x[4]) for x in plot_items]
    xticks1 =[x[5] for x in plot_items]

    ind = np.arange(len(i_arr0))
    width = 0.20
    ax.bar(ind-0.10, i_arr0, width, label='InitTime Arrow-Spark (left)')
    ax.bar(ind-0.10, c_arr0, width, bottom=i_arr0, label='ComputeTime Arrow-Spark (left)')

    ax.bar(ind+0.10, i_arr1, width, label='InitTime Spark (right)')
    ax.bar(ind+0.10, c_arr1, width, bottom=i_arr1, label='ComputeTime Spark (right)')

    plt.xticks(ind, xticks0)
    ax.set(xlabel=ovar.axis_description, ylabel='Average Execution Time (s)', title='Execution Time Composition')
        
    # ax[1].plot(list(range(10)), list(range(10)), label='Spark')

    # ax[0].set(xlabel='Time (s)', ylabel='Probability density', title='Total execution time for Arrow-Spark')
    if large:
        plt.legend(loc='best', fontsize=18, frameon=False)
    else:
        plt.legend(loc='best', frameon=False)

    if large:
        fig.set_size_inches(12, 9)

    fig.tight_layout()

    if store_fig:
          storer.store(resultdir, 'barplot', filetype, plt)

    if large:
        plt.rcdefaults()

    if not no_show:
        plt.show()