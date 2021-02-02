import time

import experiments.util as eu
from remote.util.deploymode import DeployMode
import util.fs as fs
import util.location as loc
from util.printer import *



# Perform an experiment with given parameters
def do_experiment(impl, metadeploy, reservation, node, extension, compression, amount_multiplier, kind, rb, partitions_per_node):
    # We try to do the experiment a given number of times
    # Each time we crash, we compute how many results we are missing 
    # and try to do that many runs in the next iteration.
    status = True

    # Generate to /local
    deploypath = loc.get_node_data_dir(DeployMode.LOCAL)
    data_runargs = impl.data_args.format(node*partitions_per_node, deploypath, extension, impl.amount, compression)
    extension_filepath = extension
    if impl.first_time_force:
        data_runargs += ' -gf'
    if impl.random_data:
        data_runargs += ' -gr'
        extension_filepath = 'rnd'+extension_filepath
    if extension == 'pq' and compression != 'uncompressed':
        extension_filepath = '{}_{}'.format(extension_filepath, compression)

    generate_cmd = '$JAVA_HOME/bin/java -jar ~/{} {} > /dev/null 2>&1'.format(impl.data_jar, data_runargs)
    if not eu.deploy_data_fast(metadeploy, reservation, generate_cmd, node, extension, impl.amount, kind, partitions_per_node, extension_filepath, amount_multiplier):
        exit(1)

    for extra_arg in ['--arrow-only', '--spark-only']:
        outputloc = fs.join(impl.resultloc(), node, partitions_per_node, extension_filepath, impl.amount*amount_multiplier, kind, '{}.{}.{}.{}.{}.res_{}'.format(node, extension, impl.amount*amount_multiplier, kind, rb, extra_arg[2]))
        runs_to_perform = impl.runs

        for x in range(impl.retries):
            partitions = node*partitions_per_node
            
            runargs = impl.args.format(kind, partitions, runs_to_perform, loc.get_node_data_dir(impl.data_deploy_mode), extension, impl.amount, amount_multiplier, compression)
            runargs += ' {}'.format(extra_arg)
            if impl.random_data:
                runargs += ' -gr'
            final_submit_opts = impl.submit_opts.format(outputloc)+' --conf \'spark.sql.parquet.columnarReaderBatchSize={}\''.format(rb)
            if not metadeploy.deploy_application(reservation, impl.jar, impl.mainclass, runargs, impl.extra_jars, final_submit_opts, impl.no_results_dir, impl.flamegraph_time):
                printe('!! Fatal error when trying to deploy application !! ({})'.format(outputloc))
                status = False
                break

            if metadeploy.block(eu.blockfunc, args=(metadeploy, outputloc, runs_to_perform), sleeptime=impl.appl_sleeptime, dead_after_retries=impl.appl_dead_after_tries):
                break # We are done!
            else: # Something bad happened. Do remaining runs in next iteration
                finished_runs = eu.check_num_results(outputloc)
                runs_to_perform -= (finished_runs-1) #-1 because we need a 'cold buffer' run before going on
                outputloc += '_'+str(x)
            if x == impl.retries-1:
                metadeploy.eprint('\n\n!!!FATALITY!!! for {}\n\n'.format(outputloc))
                status = False
                break
    return status

def iterate_experiments(impl, metadeploy):
    for partitions_per_node in impl.partitions_per_nodes:
        for rb in impl.rbs:
            for extension in impl.extensions:
                for compression in impl.compressions:
                    for node in impl.nodes:
                        reservation = metadeploy.cluster_start(impl.reserve_time, impl.config.format(node), impl.debug_mode, str(impl.cluster_deploy_mode), impl.no_interact)
                        if not reservation:
                            printe('!! Fatal error when trying to start cluster !! ({})'.format(outputloc))
                            return False
                        time.sleep(5) #Give slaves time to connect to master
                        for amount_multiplier in impl.amount_multipliers:
                            for kind in impl.kinds:
                                metadeploy.clean_junk(reservation, deploy_mode=impl.cluster_deploy_mode)
                                do_experiment(impl, metadeploy, reservation, node, extension, compression, amount_multiplier, kind, rb, partitions_per_node)
                        metadeploy.cluster_stop(reservation)