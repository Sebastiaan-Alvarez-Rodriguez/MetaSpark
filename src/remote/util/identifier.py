import os
import socket

# Returns number of allocated nodes, as given by the SLURM_NNODES environment variable
def num_nodes():
    return int(os.environ['SLURM_NNODES'])

# Returns max number of processes per node (e.g. returns 2 if we have 2 servers and 1 client)
# Note: Should return 1 if we have 1 server and 2 clients
def num_procs_per_node():
    return int(os.environ['SLURM_NPROCS']) // num_nodes()


# During identification, we make the following dangerous assumption:
# 'prun cpu_ranks are distributed in increasing order on sorted node numbers'
# In practice, we assume the lowest cpu rank is on the lowest node and highest rank on highest node
# By first checking if this dangerous assumption holds, we prevent coordinational disasters
def __sanity_check(rank):
    host = socket.gethostname()
    # node117   node117   node118   node118   node119   node119
    nodenames = os.environ['HOSTS'].split()
    # node117/0 node117/1 node118/0 node118/1 node119/0 node119/1
    # prun_nodenames = os.environ['PRUN_HOSTNAMES'].split()
    # 0         1         2         3         4         5
    expected_rankings = [x for x in range(len(nodenames))]
    # 2
    expected_rank_min = nodenames.index(host)
    # 3
    # expected_rank_max = len(nodenames)-1-nodenames[::-1].index(host)
    expected_rank_max = expected_rank_min-1+num_procs_per_node()
    if rank < expected_rank_min or rank > expected_rank_max:
        raise RuntimeError('Sanity check failed! Expected host {} to have rank in [{}, {}], but found: {}'.format(host, expected_rank_min, expected_rank_max, rank))
    

# Returns a global id, i.e: Different for every process
def identifier_global():
    rank = int(os.environ['PRUN_CPU_RANK'])
    __sanity_check(rank)
    return rank

# Returns a local id, i.e: Different between processes on the same node, but potentially equivalent between 2 or more processes
def identifier_local():
    # node118 (suppose we are node118/1)
    host = socket.gethostname()
    # node117   node117   node118   node118   node119   node119
    nodenames = os.environ['HOSTS'].split()
    # node117/0 node117/1 node118/0 node118/1 node119/0 node119/1
    # prun_nodenames = os.environ['PRUN_HOSTNAMES'].split()
    # 0         1         2         3         4         5
    expected_rankings = [x for x in range(len(nodenames))]
    # 2
    expected_rank_min = nodenames.index(host)
    # 3
    # expected_rank_max = len(nodenames)-1-nodenames[::-1].index(host)
    expected_rank_max = expected_rank_min-1+num_procs_per_node()
    # 3
    rank = identifier_global()
    # 1
    local_rank = rank - expected_rank_min
    return local_rank