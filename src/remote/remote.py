# Code in this file is the entrypoint for prun calls.
# run_server() and run_client() are called as soon as allocation is done.
# Goal of this file is to guide data processing on server and client nodes.


import time

import remote.util.identifier as idr
from remote.util.syncer import Syncer
import util.fs as fs
import util.location as loc
from util.printer import *


# Controls server nodes. Executed by all servers.
def run_server(debug_mode):
    experiment = Experiment.load()
    timestamp = experiment.timestamp
    repeats = experiment.metaspark.repeats

    config = config_construct_server(experiment)
    srv.populate_config(config, debug_mode)
    if config.gid == 0:
        print('Network booted. Prime ready!')
        with open(fs.join(loc.get_cfg_dir(), '.metaspark.cfg'), 'w') as file:
            file.write('\n'.join(srv.gen_connectionlist(config, experiment)))

    syncer = Syncer(config, experiment, 'server', debug_mode)

    # All servers must generate a config
    srv.gen_zookeeper_config(config)
    # All servers must write their myid file
    srv.prepare_datadir(config)

    # If cleaning is requested, boot cleaner service
    if experiment.server_periodic_clean > 0:
        clean_repeater = Repeater(lambda: srv.clean_data(config), experiment.server_periodic_clean)
        clean_repeater.start()

    global_status = True
    for repeat in range(repeats):

        # Make/clean a directory on a local node disk to log quickly.
        # Only one process on a node has to do this
        if config.lid == 0:
            fs.rm(loc.get_node_log_dir(), ignore_errors=True)
            fs.mkdir(loc.get_node_log_dir(), exist_ok=True)

        # Must wait and synchronise with all servers and clients
        syncer.sync()
        

        local_log = fs.join(loc.get_node_log_dir(), 'server{}.log'.format(config.gid))
        executor = srv.boot(config, local_log)

        experiment.experiment_server(config, executor, repeat, lambda: srv.is_leader(local_log))
        status = srv.stop(executor)
        
        global_status &= status

        # Write server log to zookeeper/metaspark/results/<repeat>/
        if fs.isfile(local_log):
            fs.mv(local_log, fs.join(loc.get_metaspark_results_dir(), timestamp, repeat, fs.basename(local_log)))
        
        if config.gid == 0:
            prints('Server iteration {}/{} complete'.format(repeat+1, repeats))
        
        # We log failed executions in a results/<timestamp>/failures.metalog
        if not status:
            printw('Server {} status in iteration {}/{} not good'.format(config.gid, repeat, repeats-1))
            with open(fs.join(loc.get_metaspark_results_dir(), timestamp, 'failures.metalog'), 'a') as file:
                file.write('server:{}:{}\n'.format(config.gid, repeat))

    # If cleaning is requested, stop cleaner service
    if experiment.server_periodic_clean > 0:
        clean_repeater.stop()

    syncer.close()

    # Delete sync dir and communication file
    if config.gid == 0:
        fs.rm(loc.get_cfg_dir(), '.metaspark.cfg')

    return global_status


# Controls client nodes. Executed by all clients.
def run_client(debug_mode):
    experiment = Experiment.load()
    timestamp = experiment.timestamp
    repeats = experiment.metaspark.repeats

    #  We must wait until the servers make themselves known
    while not fs.isfile(loc.get_cfg_dir(), '.metaspark.cfg'):
        time.sleep(1)
    # We read the serverlist
    with open(fs.join(loc.get_cfg_dir(), '.metaspark.cfg'), 'r') as file:
        # <node101>:<clientport1>
        hosts = [line.strip() for line in file.readlines()]
    
    config = config_construct_client(experiment, hosts)

    syncer = Syncer(config, experiment, 'client', debug_mode)

    global_status = True
    for repeat in range(repeats):
        # We make a directory on a local node disk to log quickly.
        # Only one process on a node has to do this
        if config.lid == 0:
            fs.mkdir(loc.get_node_log_dir(), exist_ok=True)

        # Must wait and synchronise with all servers and clients
        
        if debug_mode: print('Client {} stage PRE_SYNC'.format(idr.identifier_global()))
        syncer.sync()
        if debug_mode: print('Client {} stage POST_SYNC'.format(idr.identifier_global()))
        
        executor = cli.boot(config, experiment, repeat)
        experiment.experiment_client(config, executor, repeat)
        status = cli.stop(executor)
        global_status &= status

        local_log = fs.join(loc.get_node_log_dir(), '{}.log'.format(config.gid))
        # Move the client log file (if it exists) from local disk to shared storage
        if fs.isfile(local_log):
            fs.mv(fs.join(loc.get_node_log_dir(), local_log), fs.join(loc.get_metaspark_results_dir(), timestamp, repeat, fs.basename(local_log)))

        # We log failed executions in a results/<timestamp>/failures.metalog
        if not status:
            printw('Client {} status in iteration {}/{} not good'.format(config.gid, repeat+1, repeats))
            with open(fs.join(loc.get_metaspark_results_dir(), timestamp, 'failures.metalog'), 'a') as file:
                file.write('server:{}:{}\n'.format(config.gid, repeat))
    syncer.close()
    return global_status