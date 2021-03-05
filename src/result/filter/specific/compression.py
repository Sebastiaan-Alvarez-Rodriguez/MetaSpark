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

    path = fs.join(loc.get_metaspark_results_dir(), resultdir)

    num_ovars = Dimension.num_open_vars(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb)
    if num_ovars > 2 or num_ovars < 2:
        print('Too {} open variables: Have {} (need open compression and amount)'.format('many' if num_ovars > 2 else 'few', ', '.join([str(x) for x in Dimension.open_vars(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb)])))
        return

    if large:
        fontsize = 28
        font = {
            'family' : 'DejaVu Sans',
            'size'   : fontsize
        }
        plt.rc('font', **font)
    plt.rc('axes', axisbelow=True)

    ovars = Dimension.open_vars(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb)
    has_compression = 'compression' in (x.name for x in ovars)
    has_amount = 'amount' in (x.name for x in ovars)
    if not (has_compression and has_amount):
        print('This plot strategy is only meant for showing varying compression-settings')
        return
    ovar_amount, ovar_compression = (ovars[0], ovars[1]) if ovars[0].name=='amount' else (ovars[1], ovars[0])

    reader = Reader(path)

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
        amount0 = getattr(frame_arrow, ovar_amount.name)
        comp0 = getattr(frame_arrow, ovar_compression.name)
        data0 = frame_arrow.c_arr / 10**9
        # Box1
        amount1 = getattr(frame_spark, ovar_amount.name)
        comp1 = getattr(frame_arrow, ovar_compression.name)
        data1 = frame_spark.c_arr / 10**9
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

    plot_items.sort(key=lambda item: int(item[0])) # Will sort on x0, the amount

    for idx, who in enumerate(['Arrow-Spark', 'Default Spark']):
        fig, ax = plt.subplots()
        # print('Have {} items. Should be paired in threes (uncompressed, snappy, gzip). Gives {} pairs.'.format(len(plot_items), len(plot_items)//3))
        num_pairs = len(plot_items) // 3
        plot_items_arranged = ((plot_items[x*3], plot_items[x*3+1], plot_items[x*3+2]) for x in range(len(plot_items)//3))

        width = 0.10
        indices = np.arange(num_pairs)+1
        compressions = ['uncompressed', 'gzip', 'snappy']
        colors = [colors[0], colors[1], colors[2]]
        bplots= []
        for idx2, (color, dataset) in enumerate(zip(colors, ([x[2+idx] for x in plot_items if x[1]==token] for token in compressions))):
            err0 = ([(np.median(x)-np.percentile(x, 1)) for x in dataset], [abs(np.median(x)-np.percentile(x, 99)) for x in dataset])
            bplots.append(ax.bar(indices+0.10*(idx2-1), [np.median(x) for x in dataset], width, yerr=err0, capsize=6, label=who, color=color, alpha=0.75, edgecolor='black'))

        plt.xticks(indices, labels=['{:.1f}'.format(x[0]*4*8/1024/1024/1024) for x in plot_items[::3]])
        ax.set(xlabel=ovar_amount.axis_description+' [GB]', ylabel='Execution Time [s]') #title='Execution Time for {}'.format(who)

        plt.legend([x[0] for x in bplots], compressions, loc='best', ncol=3)

        ax.set_ylim(bottom=0)

        if large:
            fig.set_size_inches(16, 8)

        fig.tight_layout()

        if store_fig:
              storer.store(resultdir, 'boxplot_compression_{}'.format(who.replace(' ', '-').split('-')[0].lower()), filetype, plt)

        if not no_show:
            plt.show()

    if large:
        plt.rcdefaults()