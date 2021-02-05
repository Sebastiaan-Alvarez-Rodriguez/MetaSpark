# Experiments
Deploying experiments is a useful future, as it allows much greater and custom control over the clusters and all interactions with it.
In this readme, we show how to setup an experiment.

## Empty Starting Point
Here is  an empty experiment, containing all required functions:
```python
from experiments.interface import ExperimentInterface
from dynamic.metadeploy import MetaDeployState


def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return EmptyExperiment

class EmptyExperiment(ExperimentInterface):
    '''A new experiment.'''

    def start(self, metadeploy):
        # TODO: do some experiment
        pass

    def stop(self, metadeploy):
        # TODO: cleanup
        pass
```

The `start` function is called when experimentation should start, and the `stop` function when the experiment should be halted (obviously).

## Starting a cluster
Reserving and starting a Spark cluster is the first thing most people do when starting an experiment. Doing so is simple:
```python
metadeploy.cluster_start(time_to_reserve, config_filename, debug_mode, deploy_mode, no_interact, launch_spark=True)
```
We must specify
 - Time to reserve. Formatted as [dd:[hh:[mm:ss]]]. Please mind your peers on the DAS5.
 - Config to use. Configurations contain the number of nodes to reserve, and whether to communicate over infiniband or not. They are located in `MetaSpark/conf/cluster/` once you have started the program and answered a few questions
 - Whether to print debugging information during the lifetime of the cluster. We encourage you to turn this off.
 - Where to place Spark workdirs. Spark advocates these workdirs should be placed in fast, non-volatile storage. We suggest you use DeployMode.LOCAL or faster
 - Whether you want interaction or not. In case you want to run experiments unsupervised, we highly suggested you turn no_interact on 
 - Whether you want to launch a Spark cluster on the reservation. You probably should do that.


## Deploying Data
Many people use Spark to process some amount of data. Data is commonly placed in files, in local disks available to reserved nodes.
After you start a cluster, you probably want to distribute or generate some data across nodes. Here is how you can do that.

#### Copying data to all nodes
If we really need to copy data from the Spark master node to every executor node, we execute:
```python
metadeploy.deploy_data(reservation, ['/local/username/bigdata0.abc', '/local/username/bigdatafolder/'], DeployMode.RAM, skip=True, subpath='/sub/')
```
This places given file `/local/username/bigdata0.abc` and directory `/local/username/bigdatafolder/` (which are local on the DAS5 main node), inside the `RAMDisk` of every allocated node.
`RAMDisk` is a memory-mapped filesystem found on every node, in `/dev/shm`.
The exact target location for files depends on the given [/src/remote/util/DeployMode](/src/remote/util/deploymode.py):
 - `STANDARD` means "copy to NFS mount". Does not cause any copies. If you want to do a copy to NFS mount, just write some code for that yourself.
 - `LOCAL` copies to `/local/username/data/`
 - `LOCAL_SSD` copies to `/local-ssd/username/data/`
 - `RAM` copies to RAMDISK, at `/dev/shm/username/data/` 

Finally, the `skip` parameter determines whether we will skip copying data if it already is found in the target location.

#### Locally copying data
If we already have the data on a local drive available for each node, but we want the data to exist on some other place in the node, we could use:
```python
metadeploy.deploy_nonspark_application("cp /local/hugedatafile.abc /local-ssd/username/")
```
The previous `deploy_data` method we discussed cannot do this. That function only can have sources available to the *main* DAS5 node, because that function is executed on the *main* node.
This function is executed on each *reserved* node, and hence can access data only available to a *reserved* node.

#### Generate data
If we have no data, but instead some executable on a shared drive that generates data, we use:
```python
metadeploy.deploy_nonspark_application("~/generation/generator testarg --amount 50000 -n 10 --some-other-param")
```
This executes the generator executable on every reserved node. Depending on how difficult it is to generate data, this is commonly the fastest method.


#### Generate data, triangular deployment
Triangular deployment is a feature mainly useful for when we use temporary or volatile storage, such as RAMDisk.
The problem is that RAMDisk should be cleaned up after you are done with your experiment.
In case you generate data, you would need to *re-generate* the data to RAMDisk *every time* you want to repeat the experiment at some later point in time.

If you expect you will be reusing data a lot, and deploy on RAMDisk, use a command resembling function `deploy_data_fast` in [experiments/util](/experiments/util.py).
As you can read there, we first generate data to the local disk of every node, if the data is not stored there already.
When the data is in `/local`, we copy the data to RAMDisk.

Next time when we want to experiment with the same data, we find the data already available in `/local`, and only have to copy to RAMDisk.


## Application Deployment
We are almost there now. We covered reserving nodes and deploying a cluster. Next, we discussed several ways of getting data on reserved nodes where you need it.
Finally, we must submit an application to Spark to start distributed processing.

#### Regular
The most basic example would be this:
```python
metadeploy.deploy_application('~/myjar.jar', 'com.test.HelloWorld', args, extra_jars, submit_opts, no_resultdir)
```
The arguments are:
 - Full path to jarfile.
 - Fully qualified path to main class in JAR from JAR root. Normally looks like `org.something.SomeClass`.
 - Commandline arguments for your `main()` function in your mainclass.
 - List of extra jars to submit. Useful if you have a bunch of dependencies outside of your main JAR.
 - Raw submission options passed to `spark-submit`. Only for advanced users.
 - Boolean flag indicating whether we want a resultdir or not. If set, generates a timestamped directory in `MetaSpark/results/`

#### Debugging
Sometimes, some really weird stuff happens, because we made a simple mistake, e.g. our application seems to do its work, but does not print results (e.g. because we forgot to make one map thread-safe).
Java provides the Java Flight Recording (JFR) api. This api often pauses the VM, checks the current nested stackframes, checks memory usage, locks held, threads blocked, GC, CPU usage etc. It gives us method profiling.

We have built-in support for making Java Flight Recordings of Spark executors.
Using it is simple:
```python
deploy_application(jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir, flamegraph='3m')
```
Most arguments are the same as above. However, we use the flamegraph option here. It is set for `3m`, which means we want a recording lasting either 3 minutes, or until the end of execution, whichever occurs first.
All recordings for the current deployment call are stored in a timestamped directory in `MetaSpark/recordings/`.

In order for the flight recorder to work, you have to set:
```python
submit_opts = '\
--conf \'spark.executor.extraJavaOptions=-XX:+FlightRecorder\' \
--conf \'spark.driver.extraJavaOptions=-XX:+FlightRecorder\' \
'
```
This enables the flight recorder in the JVM of each node.

Once you have your recordings, you can open these (`.jfr`) files using [this for openJDK](https://adoptopenjdk.net/jmc.html). If you use other java distributions than openJDK, see [here](https://stackoverflow.com/questions/36483804/where-to-find-java-mission-control-and-visualvm-on-ubuntu-openjdk-8) for similar programs.

> Note: You are advised to make the driver sleep for 4 to 6 seconds right after creating a `SparkSession` object, so the `jcmd` has time to find and attach to the Spark workers before they start executing.

> Note: There are some pretty good explanations about using method profiling to construct flamegraphs. The openJDK program from above can make them for you. [Here](http://www.brendangregg.com/flamegraphs.html
) is some good information on flamegraphs and their use.

## Waiting for a deployed application to finish
Control is directly returned from `deploy_application` calls once the submission is complete.
Normally, this takes up to 10 seconds, depending on the size of your jarfile.
After those 10 seconds, your program will probably not be done.
It is up to *you* to check whether your deployment is done, and block until it is done.

We provide a simple blocking function:
```python
metadeploy.block(command, args=None, sleeptime=60, dead_after_retries=3)
```
The arguments are as follows
 - Command to execute (a python function name). Must return a `MetaDeployState` found in [src/dynamic/metadeploy](/src/dynamic/metadeploy.py)
 - Arguments to pass to the supplied command. If multiple arguments, use a tuple like `(1, "a", 2.5)`
 - Time to sleep between checks
 - After how many checks we consider an application as 'dead' and stop blocking

This function returns `True` when we completed blocking because the supplied `command` function returns `MetaDeployState.COMPLETE`. Returns `False` when `command` function returns `MetaDeployState.FAILED`.


In case your application provides some kind of line-based output, (e.g. it fills a CSV file), we provide a good function to help you along in [experiments/util](/experiments/util.py):
```python
def blockfunc(metadeploy, outputloc, lines_needed):
    numlines = check_num_results(outputloc)
    metadeploy.eprint('PING: Have {}/{} results for loc {}'.format(numlines, lines_needed, outputloc))
    if numlines >= lines_needed:
        return MetaDeployState.COMPLETE, numlines
    else:
        return MetaDeployState.BUSY, numlines

```
It is pretty straightforward: This function checks if there are enough lines in the output file for our program.


## Cleaning up
After we have reserved a cluster, booted Spark, deployed data, ran our application, we finally should clean up.
The `stop` function in an experiment might be a good place to do this.

We provide a simple cleanup-function:
```python
metadeploy.clean_junk(fast=False, datadir=None)
```
It cleans the Spark work directories, wherever they are located. Note that it is not a good idea to call this function while the cluster is still doing something important.
There are 2 optional arguments:
 - If `fast` flag is set, and we have to clean up worker directories on reserved nodes, we skip doing that, because it uses `ssh` and is a bit slow. If `fast` is turned off, we clean up, wherever the worker directories live.
 - Datadir is a path to also clean up when cleaning up directories. We always assume this path is found on reserved nodes (not the main DAS5 node), and we clean it only when `fast` is set.

## Stopping the cluster
After we cleaned up the data on the reserved nodes, it is time to release these nodes back to the public.
We use:
```python
metadeploy.cluster_stop(silent=False)
```
This stops the Spark cluster and releases the reserved nodes.
The `silent` flag can optionally be set to prevent shutdown messages from being printed in your terminal.