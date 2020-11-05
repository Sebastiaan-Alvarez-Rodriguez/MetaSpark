# MetaSpark
Framework to launch Spark clusters on national Dutch clustercomputer network DAS 

## Requirements
This framework runs in python `3.5` and lower, so that you can use this framework directly on DAS5, without `venv`s and such. The only requirement is `Python >=3.2, <=3.6`.

## Installing
Simply run the following to initialize the system on DAS5:
```bash
python3 main.py --init
```

## Usage
Generally, you will want to *spawn* a cluster, and then *deploy* an application on it.
To spawn a cluster, use:
```bash
python3 main.py --remote
```
When the cluster is ready, it prints out its master url, which we need when deploying.
To deploy a cluster, use:
```bash
python3 main.py deploy jarfile mainclass master_url --args foo bar 42 bazz
```
The jarfile is the jarfile you wish to deploy.
mainclass is the package-path to the class with a main function you wish to run.
The master_url is the url we see printed when spawning a cluster.
Args are arguments that we pass on to your jarfile, and is optional.  

Use the following command to see all available options for spawning a cluster:
```bash
python3 main.py -h
```
Use the following command to see all available options for deploying a cluster:
```bash
python3 main.py deploy -h
```