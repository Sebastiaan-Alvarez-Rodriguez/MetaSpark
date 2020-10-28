from collections import defaultdict
import matplotlib.pyplot as plt 
import numpy as np 

from result.throughput.assembler import Assembler
from result.throughput.reader import Reader
import result.util.storer as storer 
from result.util.tabler import table_open
import util.fs as fs
import util.location as loc 
from util.printer import *


# We process throughput experiment results here
def throughput(logdir, large, no_show, store_fig, filetype, original):
    seconds = 5     #Amount of seconds client runs each run, for each read ratio
    path = fs.join(loc.get_metaspark_results_dir(), logdir)

    if large: 
        fontsize = 24
        font = {
            'family' : 'DejaVu Sans',
            'weight' : 'bold',
            'size'   : fontsize
        }
        plt.rc('font', **font)


    #Do we want to process the results according to original experiment? (1 run)
    if original:
        runs = sorted(list(fs.ls(path, only_dirs=True, full_paths=True)), key=lambda x: int(fs.basename(x).split('_')[1]))
        servers = []
        for run in runs:
            reader = Reader(fs.join(run, '0'))
            #retrieve throughput (read-ratio, total_throughtput) for each server
            servers.append([(x[0][0], sum(n for _, n in x)) for x in zip(*[reader.read_ops(x) for x in range(reader.num_files)])])

        fig, ax = plt.subplots()
        colors = ['r', 'lime', 'b', 'magenta', 'cyan']
        numbers = ['3', '5', '7', '9', '13']
        linestyles = ('solid', 'dashed', 'dotted', (0, (1,1)), 'dashdot')
        for server, color, i, linestyle in zip(servers, colors, numbers, linestyles):
            ax.plot([int(x) for x,_ in server], [x/seconds for _, x in server], color=color, linestyle=linestyle, label='{} servers'.format(i))
        prop = {}
        if large:
            prop = {'size': 20}
            
        ax.set_ylim([0,400000])
        ax.legend(loc='upper left', prop=prop, frameon=False)
        ax.set(xlabel='Read Ratio', ylabel='Operations per Second', title='Throughput')
        xticks = [int(x) for x,_ in servers[0]]
        ticks_pos = xticks
        plt.xticks(ticks_pos, xticks)

        if large:
            fig.set_size_inches(12, 8)

    else:  
        assembler = Assembler(path)
        #retrieve throughput (read-ratio, total_throughput)
        frames = [(x[0][0], [n for _, n in x]) for x in zip(*list(assembler.read_ops()))]
        percentiles = [1, 25, 50, 75, 99]
        print('Horizontal: percentiles\nVertical: Read ratios')
        with table_open(len(percentiles)+1) as table:
            table.write_elem('-')
            table.write_elems(percentiles)
            for x in frames:
                table.write_elem(x[0])
                nparr = np.array(x[1]) #Convert to np-array once instead of for every column
                for y in percentiles:
                    table.write_elem(np.percentile(np.array(nparr)//seconds, y, interpolation='nearest'))

        data = [np.array(x)/seconds for _, x in frames]
        ticks = ['']
        ticks.extend([x for x, _ in frames])
        fig, ax = plt.subplots()
        bplot = ax.boxplot(data, patch_artist=True)
        plt.setp(bplot['boxes'], color='lightgreen')
        plt.setp(bplot['boxes'], edgecolor='black')
        plt.setp(bplot['medians'], color='forestgreen')
        plt.xticks(np.arange(len(ticks)), labels=ticks)
        
        ax.set(yscale='log', xlabel='Read Ratio', ylabel='Operations per Second', title='Throughput')

        if large:
            fig.set_size_inches(10, 8)

    fig.tight_layout()

    if store_fig:
       storer.store('throughput', logdir, filetype, plt)

    if large:
        plt.rcdefaults()

    if not no_show:
        plt.show()