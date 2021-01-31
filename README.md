# MetaSpark
Framework to launch Spark clusters on national Dutch clustercomputer network DAS 

## Requirements
This framework runs in python `3.5` and lower, so that you can use this framework directly on DAS5, without `venv`s and such. The only requirement is `Python >=3.2, <=3.6`.

## Installing
Simply run the following to initialize the system on DAS5:
```bash
python3 main.py init
```

## Usage
Generally, you will want to *spawn* a cluster, and then *deploy* an application on it.
To spawn a cluster, use:
```bash
python3 main.py remote start
```
When the cluster is ready, it prints out its master url, which we need when deploying.
To deploy a cluster, use:
```bash
python3 main.py deploy jarfile mainclass --args foo bar 42 bazz
```
Here:
 1. jarfile is the jarfile you wish to deploy.
 2. mainclass is the package-path to the class with a main function you wish to run.
 3. Args are arguments that we pass on to your jarfile, and is optional (hence the '--')

Use the following command to see all available top-level commands for MetaSpark:
```bash
python3 main.py -h
```
To get more detailed help for a command, type the command, followed by `-h`. E.g:
```bash
python3 main.py deploy -h
```

### Example usage
Here, we give a valid and complete example of how to deploy on the DAS5. We assume that:
 1. you have an ssh key ready with name `das5key`.
 2. you have DAS5 username `SOME_USER`
 3. you have downloaded `scopt_2.12-3.7.1.jar` and 
 `spark-examples_2.12-3.0.1.jar` to directory `<project_root>/jars/`.

Here we go:
```bash
python3 main.py init
python3 main.py remote start
python3 main.py deploy spark-examples_2.12-3.0.1.jar \
org.apache.spark.examples.SparkPi \
--args "10"
```


## Improved Usage
Sometimes, we want to run experiments on the DAS5. We need greater control over the process.
For that, we have the `deploy meta` extension. In this extension, you can run Python3 code on the DAS5 main node to:
 - spawn clusters,
 - deploy data
 - wait until execution completes
 - clean up after experiment completes

Also, you can write Python3 code to perform your own tasks as well.
Finally, it is possible to perform multiple experiments one after the other, in one command.

For more information about the improved usage, check [experiments/README.md](/experiments/README.md).