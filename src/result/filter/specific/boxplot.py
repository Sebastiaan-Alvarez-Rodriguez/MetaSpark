import matplotlib.pyplot as plt
import numpy as np

from result.util.dimension import Dimension
from result.util.reader import Reader
import result.util.storer as storer
import util.fs as fs
import util.location as loc

# Plots execution time with variance (percentiles) as a boxplot, using provided filters
def stats(resultdir, node, partitions_per_node, extension, amount, kind, rb, large, no_show, store_fig, filetype, skip_internal):
    path = fs.join(loc.get_metaspark_results_dir(), resultdir)

    if Dimension.num_open_vars(node, partitions_per_node, extension, amount, kind, rb) > 1:
        print('Too many open variables: {}'.format(', '.join([str(x) for x in Dimension.open_vars(node, partitions_per_node, extension, amount, kind, rb)])))
        return

    if large:
        fontsize = 24
        font = {
            'family' : 'DejaVu Sans',
            'weight' : 'bold',
            'size'   : fontsize
        }
        plt.rc('font', **font)
        

    ovar = Dimension.open_vars(node, partitions_per_node, extension, amount, kind, rb)[0]

    if ovar.name != "amount":
        print('This plot strategy is only meant for showing varying amount-settings')
        return

    reader = Reader(path)
    fig, ax = plt.subplots()

    plot_items = []
    for frame_arrow, frame_spark in reader.read_ops(node, partitions_per_node, extension, amount, kind, rb):
        # BAR1loc
        x0 = getattr(frame_arrow, str(ovar))
        data0 = np.add(frame_arrow.i_arr, frame_arrow.c_arr) / 1000000000
        # BAR2loc
        x1 = getattr(frame_spark, str(ovar))
        data1 = np.add(frame_spark.i_arr, frame_spark.c_arr) / 1000000000
        plot_items.append((x0, data0, data1,))

    if len(plot_items) == 0:
        print('No results to plot. Exiting now...')
        return

    plot_items.sort(key=lambda item: int(item[0])) # Will sort on x0. x0==x1==ovar, the open variable

    # bplot0 = ax[0].boxplot([x[1] for x in data0], positions=[x[0] for x in data0], patch_artist=True)
    bplot0 = ax.boxplot([x[1] for x in plot_items], patch_artist=True, whis=[1,99])
    plt.setp(bplot0['boxes'], color='lightgreen')
    plt.setp(bplot0['boxes'], edgecolor='black')
    plt.setp(bplot0['medians'], color='forestgreen')
    


    bplot1 = ax.boxplot([x[2] for x in plot_items], patch_artist=True, whis=[1,99])
    plt.setp(bplot1['boxes'], color='lightcoral')
    plt.setp(bplot1['boxes'], edgecolor='black')
    plt.setp(bplot1['medians'], color='indianred')
    plt.xticks(np.arange(len(plot_items))+1, labels=[ovar.val_to_ticks(x[0]) for x in plot_items])


    # ax.set(xscale='log', yscale='log', xlabel=ovar.axis_description, ylabel='Execution Time (s)', title='Execution Time with Variance for Arrow-Spark')
    ax.set(xlabel=ovar.axis_description, ylabel='Execution Time (s)', title='Execution Time for Arrow-Spark')

    # ax[0].set(xlabel='Time (s)', ylabel='Probability density', title='Total execution time for Arrow-Spark')
    # if large:
    #     fig.legend(loc='right', fontsize=18, frameon=False)
    # else:
    #     fig.legend(loc='right', frameon=False)

    if large:
        fig.set_size_inches(10, 8)

    fig.tight_layout()

    if store_fig:
          storer.store(resultdir, 'barplot', filetype, plt)

    if large:
        plt.rcdefaults()

    if not no_show:
        plt.show()