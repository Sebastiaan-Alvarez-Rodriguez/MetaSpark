import matplotlib.pyplot as plt
import numpy as np

from result.util.dimension import Dimension
from result.util.reader import Reader
import result.util.storer as storer
import util.fs as fs
import util.location as loc

# Plots execution time with variance (percentiles) as a boxplot, using provided filters
def stats(resultdir, node, partitions_per_node, extension, compression, amount, kind, rb, large, no_show, store_fig, filetype, skip_internal):
    path = fs.join(loc.get_metaspark_results_dir(), resultdir)

    num_ovars = Dimension.num_open_vars(node, partitions_per_node, extension, compression, amount, kind, rb)
    if num_ovars > 2 or num_ovars < 2:
        print('Too {} open variables: Have {} (need open compression and amount)'.format('many' if num_ovars > 2 else 'few', ', '.join([str(x) for x in Dimension.open_vars(node, partitions_per_node, extension, compression, amount, kind, rb)])))
        return

    if large:
        fontsize = 24
        font = {
            'family' : 'DejaVu Sans',
            'weight' : 'bold',
            'size'   : fontsize
        }
        plt.rc('font', **font)
        

    ovars = Dimension.open_vars(node, partitions_per_node, extension, compression, amount, kind, rb)
    has_compression = 'compression' in (x.name for x in ovars)
    has_amount = 'amount' in (x.name for x in ovars)
    if not (has_compression and has_amount):
        print('This plot strategy is only meant for showing varying compression-settings')
        return
    ovar_amount, ovar_compression = (ovars[0], ovars[1]) if ovars[0].name=='amount' else (ovars[1], ovars[0])

    reader = Reader(path)
    fig, ax = plt.subplots()

    plot_items = []
    for frame_arrow, frame_spark in reader.read_ops(node, partitions_per_node, extension, compression, amount, kind, rb):
        if frame_arrow.tag != 'arrow':
            print('Unexpected arrow-tag: '+str(frame_arrow.tag))
            return
        if frame_spark.tag != 'spark':
            print('Unexpected spark-tag: '+str(frame_spark.tag))
            return
        # Box0
        amount0 = getattr(frame_arrow, ovar_amount.name) / 10**9
        comp0 = getattr(frame_arrow, ovar_compression.name)
        data0 = np.add(frame_arrow.i_arr, frame_arrow.c_arr) / 10**9
        # Box1
        amount1 = getattr(frame_spark, ovar_amount.name) / 10**9
        comp1 = getattr(frame_arrow, ovar_compression.name)
        data1 = np.add(frame_spark.i_arr, frame_spark.c_arr) / 10**9
        if amount0 != amount1:
            print('Unexpected amount-identifier mismatch!')
            return
        if comp0 != comp1:
            print('Unexpected compression-identifier mismatch!')
            return
        plot_items.append((amount0, comp0, data0, data1,))

    if len(plot_items) == 0:
        print('No results to plot. Exiting now...')
        return

    plot_items.sort(key=lambda item: int(item[0])) # Will sort on x0. x0==x1==ovar, the open variable

    # plot_items: (am, cm), 
    print('Have {} items. Should be paired in threes (uncompressed, snappy, gzip). Gives {} pairs.'.format(len(plot_items), len(plot_items)//3))
    plot_items_arranged = ((plot_items[x*3], plot_items[x*3+1], plot_items[x*3+2]) for x in range(len(plot_items)//3))

    # Plot the left boxes, the uncompressed ones
    bplot0 = ax.boxplot([x[2] for x in plot_items if x[1]=='uncompressed'], patch_artist=True, whis=[1,99], widths=(np.full(len(plot_items)//3, 0.3)), positions=np.arange(5)+1-0.3) #positions=np.arange(len(plot_items))+1-0.15
    plt.setp(bplot0['boxes'], color='steelblue', alpha=0.75, edgecolor='black')
    plt.setp(bplot0['medians'], color='midnightblue')

    # Plot the middle boxes, the gzip ones
    bplot1 = ax.boxplot([x[2] for x in plot_items if x[1]=='gzip'], patch_artist=True, whis=[1,99], widths=(np.full(len(plot_items)//3, 0.3)), positions=np.arange(5)+1) #positions=np.arange(len(plot_items))+1-0.15
    plt.setp(bplot1['boxes'], color='lightgreen', alpha=0.75, edgecolor='black')
    plt.setp(bplot1['medians'], color='forestgreen')

    bplot2 = ax.boxplot([x[2] for x in plot_items if x[1]=='snappy'], patch_artist=True, whis=[1,99], widths=(np.full(len(plot_items)//3, 0.3)), positions=np.arange(5)+1+0.3) #positions=np.arange(len(plot_items))+1-0.15
    plt.setp(bplot2['boxes'], color='lightcoral', alpha=0.75, edgecolor='black')
    plt.setp(bplot2['medians'], color='indianred')

    # Plot Spark stuff:
    # bplot1 = ax.boxplot([x[3] for x in plot_items], patch_artist=True, whis=[1,99], widths=(np.full(len(plot_items), 0.3)), positions=np.arange(len(plot_items))+1+0.15)
    # plt.setp(bplot1['boxes'], color='lightcoral', alpha=0.75, edgecolor='black')
    # plt.setp(bplot1['medians'], color='indianred')
    plt.xticks((np.arange(len(plot_items)//3))+1, labels=[ovar_amount.val_to_ticks(x[0]) for x in plot_items[::3]])


    ax.set(xlabel=ovar_amount.axis_description, ylabel='Execution Time [s]', title='Execution Time for Arrow-Spark')

    # add a twin axes and set its limits so it matches the first
    # ax2 = ax.twinx()
    # ax2.set_ylabel('Relative slowdown of Arrow-Spark')
    # # ax2.set_ylim((0.7, 1.0))
    # ax2.plot(np.arange(len(plot_items))+1, [np.median(x[2])/np.median(x[3]) for x in plot_items], label='Relative speedup of Arrow-Spark')
    # plt.grid()

    ax.legend([bplot0['boxes'][0], bplot1['boxes'][0], bplot2['boxes'][0]], ['uncompressed', 'gzip', 'snappy'], loc='best')

    if large:
        fig.set_size_inches(16, 9)

    fig.tight_layout()

    if store_fig:
          storer.store(resultdir, 'boxplot_cluster_scalability', filetype, plt)

    if large:
        plt.rcdefaults()

    if not no_show:
        plt.show()