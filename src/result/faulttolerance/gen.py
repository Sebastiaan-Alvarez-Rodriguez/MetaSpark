import matplotlib.pyplot as plt
import numpy as np
from itertools import zip_longest

from result.faulttolerance.assembler import Assembler, get_kill_logs
from result.faulttolerance.reader import Reader 
from result.faulttolerance.identifier import Identifier
import result.util.storer as storer
import util.fs as fs
import util.location as loc


# We process faulttolerance experiment results here
def faulttolerance(logdir, large, no_show, store_fig, filetype, original):
    time = 300
    log_time = 0.3                        # Amount of seconds between every log moment
    nr_kills = 7
    nap_time = time / (nr_kills+1)        # Amount of seconds between every 2 kills

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
        path = fs.join(loc.get_metaspark_results_dir(), logdir)
        
        #Get only one file, ignore all others
        run = next(fs.ls(path, only_dirs=True, full_paths=True))
        reader = Reader(run)
        identifier = Identifier(get_kill_logs(run))

        # Reads all log files, and adds numbers from same timestamps together in a handy one-liner
        frame = [sum(x) for x in zip_longest(*[reader.read_ops(x) for x in range(reader.num_files)], fillvalue=0)]
        leader_kills = identifier.identify_leaders()

        if len(leader_kills) != nr_kills:
            raise RuntimeError('kill-logs is ill-formated, please provide {} lines'.format(nr_kills))

        fig, ax = plt.subplots()
        ax.plot([x*0.3 for x in range(len(frame))], frame, color='tab:blue')
        killpoints_leader = [(1+x)*nap_time for x in range(nr_kills) if leader_kills[x]]
        killpoints_follower = [(1+x)*nap_time for x in range(nr_kills) if not leader_kills[x]]
        ax.scatter(killpoints_leader, [frame[int(x*(1.0/0.3))] for x in killpoints_leader], color='tab:red', marker='o', s=400, label='event: Leader Kill')
        ax.scatter(killpoints_follower, [frame[int(x*(1.0/0.3))] for x in killpoints_follower], color='tab:green', marker='o', s=400, label='event: Follower Kill')
        ax.set(xlabel='Time (seconds)', ylabel='Operations per Second', title='Operations per second around Kill Events')
        ax.legend()

    else:
        assembler = Assembler(fs.join(loc.get_metaspark_results_dir(), logdir))

        frames = list(assembler.read_ops())
        leader_kills = list(assembler.identify_leaders())

        for kill in leader_kills:
            if len(kill) != nr_kills:
                raise RuntimeError('kill-logs is ill-formated, please provide {} lines'.format(nr_kills))
        if len(leader_kills) != len(frames):
            raise RuntimeError('Missing leader kill info (or frames)')

        kill_idx_dists = nap_time / log_time  # Amount of indices between every 2 kills
        kill_idxs = [int((1+x)*kill_idx_dists) for x in range(nr_kills)] #All kill starting indices
        radius = int(kill_idx_dists // 2) # We want a window with this radius for each kill
        bin_leader = []
        bin_follower = []
        for i, kill in enumerate(kill_idxs):
            for frame, leader_kill in zip(frames, leader_kills):
                event = [frame[j] for j in range(kill - radius, kill + radius)]
                if leader_kill[i]:
                    bin_leader.append(event)
                else:
                    bin_follower.append(event)

        med_leader = []
        med_follower = []
        up_leader = []
        up_follower = []
        low_leader = []
        low_follower = []

        for i in range(int(2*radius)):
            leaders = [event[i] for event in bin_leader]
            followers = [event[i] for event in bin_follower]
            med_leader.append(np.percentile(leaders, 50))
            up_leader.append(np.percentile(leaders, 75))
            low_leader.append(np.percentile(leaders, 25))
            med_follower.append(np.percentile(followers, 50))
            up_follower.append(np.percentile(followers, 75))
            low_follower.append(np.percentile(followers, 25))

        fig, ax = plt.subplots()

        x_points = [x*log_time for x in range(-radius, radius)]

        ax.plot(x_points, (1/log_time)*np.array(med_leader), color='tab:red', label='event: kill leader')
        ax.vlines(x_points, (1/log_time)*np.array(low_leader), (1/log_time)*np.array(up_leader), color='tab:red', alpha=0.6, zorder=0)
        ax.plot(x_points, (1/log_time)*np.array(med_follower), color='tab:green', label='event: kill follower')
        ax.vlines(x_points, (1/log_time)*np.array(low_follower), (1/log_time)*np.array(up_follower), color='tab:green', alpha=0.6, zorder=0)
        ax.set(xlabel='Time (seconds)', ylabel='Operations per Second', title='Operations per second around Kill Events')

        if large:
            ax.legend(loc='lower right', fontsize=18, frameon=False)
        else:
            ax.legend(loc='lower right', frameon=False)
    
    if large:
        fig.set_size_inches(10, 8)

    fig.tight_layout()

    if store_fig:
       storer.store('faulttolerance', logdir, filetype, plt)

    if large:
        plt.rcdefaults()

    if not no_show:
        plt.show()