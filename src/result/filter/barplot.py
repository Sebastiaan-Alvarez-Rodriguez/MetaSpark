import matplotlib.pyplot as plt
import numpy as np

from result.util.reader import Reader
import result.util.storer as storer
import util.fs as fs
import util.location as loc

# https://matplotlib.org/3.1.1/gallery/lines_bars_and_markers/bar_stacked.html

def open_var(partition, extension, amount, kind, rb):
    if partition == None:
        return 'partition'
    elif extension == None:
        return 'extension'
    elif amount == None:
        return 'amount'
    elif kind == None:
        return 'kind'
    elif rb == None:
        return 'rb'
    raise RuntimeError('No open vars found')


def num_open_vars(partition, extension, amount, kind, rb):
    return [partition, extension, amount, kind, rb].count(None)

# Plots execution time, using provided filters
def stats(resultdir, partition, extension, amount, kind, rb, large, no_show, store_fig, filetype, skip_internal):
    path = fs.join(loc.get_metaspark_results_dir(), resultdir)

    if num_open_vars(partition, extension, amount, kind, rb) > 1:
        print('Too many open variables!')
        return

        if large:
            fontsize = 24
            font = {
                'family' : 'DejaVu Sans',
                'weight' : 'bold',
                'size'   : fontsize
            }
            plt.rc('font', **font)
        

    ovar = open_var(partition, extension, amount, kind, rb)

    reader = Reader(path)
    fig, ax = plt.subplots(2)

    i_arr0 = []
    c_arr0 = []
    xticks0 = []

    i_arr1 = []
    c_arr1 = []
    xticks1 = []
    for frame in reader.read_ops(partition, extension, amount, kind, rb):
        # BAR1loc
        i_arr0.append(np.average(frame.ds_i_avgtime))
        c_arr0.append(np.average(frame.ds_c_avgtime))
        xticks0.append(getattr(frame, ovar))
        # BAR2loc
        i_arr1.append(np.average(frame.spark_i_avgtime))
        c_arr1.append(np.average(frame.spark_c_avgtime))
        xticks1.append(getattr(frame, ovar))
        
        

    ind = np.arange(len(i_arr0))
    width = 0.35
    ax[0].bar(ind, i_arr0, width, label='InitTime')
    ax[0].bar(ind, c_arr0, width, bottom=i_arr0, label='ComputeTime')
    plt.sca(ax[0])
    plt.xticks(ind, xticks0)

    ax[1].bar(ind, i_arr1, width, label='InitTime')
    ax[1].bar(ind, c_arr1, width, bottom=i_arr1, label='ComputeTime')
    plt.sca(ax[1])
    plt.xticks(ind, xticks1)

    ax[0].set(ylabel='Average Execution Time (s)', title='Execution Time Composition for Dataset')
    ax[1].set(ylabel='Average Execution Time (s)', title='Execution Time Composition for Spark')
        
    # ax[1].plot(list(range(10)), list(range(10)), label='Spark')

    # ax[0].set(xlabel='Time (s)', ylabel='Probability density', title='Total execution time for Dataset')
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