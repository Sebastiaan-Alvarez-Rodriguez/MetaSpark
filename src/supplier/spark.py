# In this fiile, we provide functions to
# install and interact with Apache Spark

from pathlib import Path
import urllib.request
import shutil

import util.fs as fs
import util.location as loc
from util.printer import *


# Check if Spark is installed in deps/spark
def spark_available():
    return fs.isdir(loc.get_spark_dir()) and fs.isdir(loc.get_spark_sbin_dir())


#installs Spark
def install():
    if spark_available():
        return True

    depsloc = loc.get_metaspark_dep_dir()
    fs.mkdir(depsloc, exist_ok=True)
    print('Installing Spark in {0}'.format(depsloc))

    archiveloc = fs.join(depsloc, 'spark-3.0.1.tgz')

    if not fs.isfile(archiveloc):
        for x in range(5):
            try:
                fs.rm(archiveloc, ignore_errors=True)
                url = 'https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz'
                print('[{0}] Fetching spark from {1}'.format(x, url))
                urllib.request.urlretrieve(url, archiveloc)
                break
            except Exception as e:
                if x == 0:
                    printw('Could not download Spark. Retrying...')
                elif x == 4:
                    printe('Could not download Spark: {}'.format(e))
                    return False
    for x in range(5):
        try:
            shutil.unpack_archive(archiveloc, depsloc)
            fs.mv(fs.join(depsloc, 'spark-3.0.1-bin-hadoop2.7'), loc.get_spark_dir())
            print('installing complete')
            return True
        except Exception as e:
            if x == 4:
                printe('Could not download zip file correctly')
                return False
            elif x == 0:
                print(e)
                printw('Could not extract archive. Retrying...')
    return False