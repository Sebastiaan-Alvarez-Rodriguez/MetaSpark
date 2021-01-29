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

    if ovar.name != 'node':
        print('This plot strategy is only meant for showing varying node-settings')
        return

    reader = Reader(path)
    fig, ax = plt.subplots()

    plot_items = []
    for frame_arrow, frame_spark in reader.read_ops(node, partitions_per_node, extension, amount, kind, rb):
        # Box0
        x0 = getattr(frame_spark, ovar.name)
        data0 = np.add(frame_arrow.i_arr, frame_arrow.c_arr) / 10**9
        # Box1
        x1 = getattr(frame_spark, ovar.name)
        data1 = np.add(frame_spark.i_arr, frame_spark.c_arr) / 10**9
        plot_items.append((x0, data0, data1,))

    if len(plot_items) == 0:
        print('No results to plot. Exiting now...')
        return

    plot_items.sort(key=lambda item: int(item[0])) # Will sort on x0. x0==x1==ovar, the open variable

    # bplot0 = ax[0].boxplot([x[1] for x in data0], positions=[x[0] for x in data0], patch_artist=True)
    bplot0 = ax.boxplot([x[1] for x in plot_items], patch_artist=True, whis=[1,99], widths=(np.full(len(plot_items), 0.3)), positions=np.arange(len(plot_items))+1-0.15)
    plt.setp(bplot0['boxes'], color='lightgreen')
    plt.setp(bplot0['boxes'], edgecolor='black')
    plt.setp(bplot0['medians'], color='forestgreen')
    
    bplot1 = ax.boxplot([x[2] for x in plot_items], patch_artist=True, whis=[1,99], widths=(np.full(len(plot_items), 0.3)), positions=np.arange(len(plot_items))+1+0.15)
    plt.setp(bplot1['boxes'], color='lightcoral')
    plt.setp(bplot1['boxes'], edgecolor='black')
    plt.setp(bplot1['medians'], color='indianred')
    plt.xticks(np.arange(len(plot_items))+1, labels=[ovar.val_to_ticks(x[0]) for x in plot_items])


    # ax.set(xscale='log', yscale='log', xlabel=ovar.axis_description, ylabel='Execution Time (s)', title='Execution Time with Variance for Arrow-Spark')
    ax.set(xlabel='{} (x{:2.0e})'.format(ovar.axis_description, 10**9), ylabel='Execution Time (s)', title='Execution Time for Arrow-Spark')

    # add a twin axes and set its limits so it matches the first
    ax2 = ax.twinx()
    ax2.set_ylabel('Arrow-Spark slowdown (compared to quickest)')
    ax2.set_ylim(ax.get_ylim())

    arrow_minimal_y = np.min([np.median(x[1]) for x in plot_items])
    print('Minimal Arrow-Spark median = {}'.format(arrow_minimal_y))
    # 2nd y axis method 1: apply a function formatter to set other values than for other y-axis
    # import matplotlib.ticker as mt
    # formatter = mt.FuncFormatter(lambda x, pos: '{:.2f}'.format(x/arrow_minimal_y))
    # ax2.yaxis.set_major_formatter(formatter)
    # 2nd y axis method 2: Manually place ticks locations and labels
    ax2.yaxis.set_ticks([np.median(x[1]) for x in plot_items])
    ax2.yaxis.set_ticklabels(['{:.2f}'.format(np.median(x[1])/arrow_minimal_y) for x in plot_items])

    spark_minimal_y = np.min([np.median(x[2]) for x in plot_items])
    print('Minimal Arrow median = {}'.format(spark_minimal_y))
    # 3d y axis method:
    # https://matplotlib.org/3.1.1/gallery/ticks_and_spines/multiple_yaxis_with_spines.html
    ax3 = ax.twinx()
    ax3.set_ylabel('Spark slowdown (compared to quickest)')
    ax3.spines['right'].set_position(('axes', 1.2))
    # 4 below lines disable all frame lines for 3rd axis
    ax3.set_frame_on(True)
    ax3.patch.set_visible(False)
    for sp in ax3.spines.values():
        sp.set_visible(False)
    ax3.spines['right'].set_visible(True) # sets right frameline visible again
    ax3.set_ylim(ax.get_ylim())

    ax3.yaxis.set_ticks([np.median(x[2]) for x in plot_items])
    ax3.yaxis.set_ticklabels(['{:.2f}'.format(np.median(x[2])/spark_minimal_y) for x in plot_items])


    ax.legend([bplot0['boxes'][0], bplot1['boxes'][0]], ['Arrow-Spark', 'Spark'], loc='best')

    # if large:
    #     fig.legend(loc='right', fontsize=18, frameon=False)
    # else:
    #     fig.legend(loc='right', frameon=False)


    if large:
        fig.set_size_inches(16, 9)

    fig.tight_layout()

    if store_fig:
          storer.store(resultdir, 'barplot', filetype, plt)

    if large:
        plt.rcdefaults()

    if not no_show:
        plt.show()