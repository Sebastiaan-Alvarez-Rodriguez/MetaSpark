import os
import subprocess
import time
import sys

# Print to stderr
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def try_os_command(command, err=None):
    if err == None:
        err = 'OS problem occured with command'.format(command)
    for x in range(5):
        try:
            if os.system(command) == 0:
                return True
        except Exception as e:
            if x == 4:
                eprint('{}: {}'.format(err, e))
        time.sleep(5)
    return False

# Returns number of completed runs, or 0 if there was a problem
def check_runs(das5_loc):
    command = 'wc -l {}'.format(das5_loc)
    try:
        return int(subprocess.check_output(command, shell=True).decode('utf-8').split(' ')[0])
    except Exception as e:
        return 0

# Like the name says: Blocks until we are done or dead.
# Returns True if done, False if dead
def block_until_done_or_dead(das5_loc, runs):
    to_go = runs
    unchanged = 0
    while True:
        if to_go <= 0:
            return True #Completed!
        num_processed = check_runs(das5_loc)
        to_go_new = runs - num_processed
        eprint('PING: Need {} for loc {}'.format(to_go_new, das5_loc))
        if to_go_new == to_go: # No change since last time?
            unchanged += 1
        else:
            unchanged = 0
        to_go = to_go_new
        if unchanged > 5: #5 minutes of no changes
            eprint('Failure in location: {}'.format(das5_loc))
            return False
        time.sleep(60)

# Stop a cluster immediately
def stop_cluster(): 
    return try_os_command('python3 /var/scratch/hpcl1910/MetaSpark/main.py stop', 'Big problem stopping cluster!')


# Start a cluster with partition nodes
def start_cluster(partition):
    command = 'python3 /var/scratch/hpcl1910/MetaSpark/main.py exec -c {}.cfg -t 08:00:00 -f'.format(partition)
    if not try_os_command(command, 'Big problem starting cluster!'):
        return False
    command = 'python3 /var/scratch/hpcl1910/MetaSpark/main.py deploy data /var/scratch/hpcl1910/MetaSpark/*.pq --internal --skip'.format(partition)
    return try_os_command(command, 'Big problem distributing input data!')

# Remove junk generated during each run
def clean_junk():
    command = 'rm -rf /local-ssd/hpcl1910/work/ /var/scratch/hpcl1910/MetaSpark/deps/spark/work/ /var/scratch/hpcl1910/MetaSpark/deps/spark/logs/'
    os.system(command)

def start_application(outputloc, partition, extension, amount, kind, rb, progruns):
    clean_junk()
    command = 'python3 /var/scratch/hpcl1910/MetaSpark/main.py deploy application Hydro-1.0-all.jar experiments.Experimenter --internal --args "\
    {} -nr {} -np {} -r {} -rb {} \
    -p [[DATADIR]]/{}.{}" \
    --opts "--conf \'spark.executor.extraJavaOptions=-Dfile={}\' \
    --conf \'spark.driver.extraJavaOptions=-Dfile={}\'"'.format(
        kind, amount, partition, progruns, rb, amount, extension, outputloc, outputloc)
    for x in range(5):
        try:
            if os.system(command) == 0:
                return True
        except Exception as e:
            if x == 4:
                eprint('Big problem starting application: {}'.format(e))
        time.sleep(5)
    return False

# Returns True if we already finished this one and should skip it, False otherwise
def finished(partition, extension, amount, kind, rb):
    return False

    # nfs_old had below finished:
    # if partition == 32 and extension == 'pq' and amount == 10000:
    #     if kind == 'rdd':
    #         return True
    #     if kind == 'df' and rb <= 20480*8:
    #         return True
    # if partition == 16 and extension == 'pq' and amount == 10000:
    #     if kind == 'rdd' or kind == 'df':
    #         return True
    #     if kind == 'ds' and rb <= 20480*2:
    #         return True
    # if partition == 4 and extension == 'pq' and amount == 10000:
    #     if kind == 'rdd' and rb == 20480:
    #         return True
    # return False

def main():
    eprint('Ready to deploy!')
    partitions = [32, 16, 8, 4]
    rbs = [20480*8, 20480, 20480*2, 20480*4, 20480*16]
    amounts = [10000, 100000, 1000000, 10000000, 100000000]
    extensions = ['pq', 'csv']
    kinds = ['rdd', 'df', 'ds'] #'df_sql' is off until fixed!

    for rb in rbs:
        for extension in extensions:
            for partition in partitions:
                stop_cluster()
                if not start_cluster(partition):
                    return False
                else:
                    time.sleep(10) #Give slaves time to connect to master        
                for amount in amounts:
                    for kind in kinds:
                        if finished(partition, extension, amount, kind, rb):
                            continue
                        progruns = 100
                        runs = 2*progruns
                        outputloc = '/var/scratch/hpcl1910/{0}/{1}/{2}/{3}/{0}.{1}.{2}.{3}.{4}.res'.format(partition, extension, amount, kind, rb)
                        for x in range(3):
                            start_application(outputloc, partition, extension, amount, kind, rb, progruns)
                            if block_until_done_or_dead(outputloc, runs):
                                break # We are done!
                            else: #We died. Do remaining runs
                                finished_runs = check_runs(outputloc)
                                progruns -= (finished_runs//2) + 1 + (1 if finished_runs %2 != 0 else 0) #+1 for removing initial run, +1 for if we have an uneven amount
                                runs = 2*progruns
                                outputloc += '_'+str(x)
                            if x == 2:
                                eprint('\n\n!!!FATALITY!!! for {}\n\n'.format(outputloc))
                                print('\n\n!!!FATALITY!!! for {}\n\n'.format(outputloc))


if __name__ == '__main__':
    main()