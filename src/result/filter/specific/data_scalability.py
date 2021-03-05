import matplotlib.pyplot as plt
from matplotlib import cm
import numpy as np

from result.util.dimension import Dimension
from result.util.reader import Reader
import result.util.storer as storer
import util.fs as fs
import util.location as loc

# Plots execution time with variance (percentiles) as a boxplot, using provided filters
def stats(resultdir, num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb, large, no_show, store_fig, filetype, skip_leading):
    colormap = cm.get_cmap('winter', 5)
    colors = [colormap(2), colormap(0), 'red']
    # colors = ['deepskyblue', 'lightskyblue', 'cornflowerblue']
    path = fs.join(loc.get_metaspark_results_dir(), resultdir)
    if Dimension.num_open_vars(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb) > 1:
        print('Too many open variables: {}'.format(', '.join([str(x) for x in Dimension.open_vars(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb)])))
        return

    if large:
        fontsize = 28
        font = {
            'family' : 'DejaVu Sans',
            'size'   : fontsize
        }
        plt.rc('font', **font)
    plt.rc('axes', axisbelow=True)

    ovar = Dimension.open_vars(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb)[0]

    if ovar.name != 'amount':
        print('This plot strategy is only meant for showing varying amount-settings')
        return

    reader = Reader(path)
    fig, ax = plt.subplots()

    plot_items = []
    for frame_arrow, frame_spark in reader.read_ops(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb, skip_leading):
        if frame_arrow.tag != 'arrow':
            print('Unexpected arrow-tag: '+str(frame_arrow.tag))
            return
        if frame_spark.tag != 'spark':
            print('Unexpected spark-tag: '+str(frame_spark.tag))
            return
        if len(frame_arrow) != len(frame_spark):
            print('Warning: comparing different sizes')
        # Box0
        x0 = frame_spark.amount
        data0 = frame_arrow.c_arr / 10**9
        # Box1
        x1 = frame_spark.amount
        data1 = frame_spark.c_arr / 10**9
        plot_items.append((x0, data0, data1,))

    if len(plot_items) == 0:
        print('No results to plot. Exiting now...')
        return

    plot_items.sort(key=lambda item: int(item[0])) # Will sort on x0. x0==x1==ovar, the open variable

    width = 0.20
    indices = np.arange(len(plot_items))+1
    err0 = ([(np.median(x[1])-np.percentile(x[1], 1)) for x in plot_items], [abs(np.median(x[1])-np.percentile(x[1], 99)) for x in plot_items])
    err1 = ([(np.median(x[2])-np.percentile(x[2], 1)) for x in plot_items], [abs(np.median(x[2])-np.percentile(x[2], 99)) for x in plot_items])
    
    bplot0 = ax.bar([x-0.10 for x in indices], [np.median(x[1]) for x in plot_items], width, yerr=err0, capsize=6, label='Arrow-Spark', alpha=0.75, color=colors[0], edgecolor='black')
    bplot1 = ax.bar([x+0.10 for x in indices], [np.median(x[2]) for x in plot_items], width, yerr=err1, capsize=6, label='Spark', alpha=0.75, color=colors[1], edgecolor='black')
    plt.xticks(indices, labels=['{:.1f}'.format(x[0]*4*8/1024/1024/1024) for x in plot_items])


    # ax.set(xscale='log', yscale='log', xlabel=ovar.axis_description, ylabel='Execution Time (s)', title='Execution Time with Variance for Arrow-Spark')
    ax.set(xlabel=ovar.axis_description+' [GB]', ylabel='Execution Time [s]')

    # add a twin axes and set its limits so it matches the first
    ax2 = ax.twinx()
    ax2.set_ylabel('Relative speedup of Arrow-Spark')
    ax2.tick_params(axis='y', colors=colors[2])
    line = ax2.plot(indices, [np.median(x[2])/np.median(x[1]) for x in plot_items], label='Relative speedup of Arrow-Spark',  marker='D', markersize=10, markeredgecolor='black', markeredgewidth='1.5', color=colors[2])
    plt.grid()
    # plt.legend([bplot0['boxes'][0], bplot1['boxes'][0]], ['Arrow-Spark', 'Spark'], loc='upper left')
    plt.legend([bplot0[0], bplot1[0]], ['Arrow-Spark', 'Spark'], loc='upper left', ncol=2)

    ax.set_ylim(bottom=0)
    ax2.set_ylim(bottom=0, top=1.3)
    if large:
        fig.set_size_inches(16, 8)

    fig.tight_layout()

    if store_fig:
          storer.store(resultdir, 'boxplot_data_scalability', filetype, plt)

    if large:
        plt.rcdefaults()

    if not no_show:
        plt.show()