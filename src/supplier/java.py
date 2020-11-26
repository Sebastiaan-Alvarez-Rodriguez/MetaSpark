# In this file, we provide functions to interact with Java
# We explicitly do NOT provide ways to install Java,
# as it is already installed on the DAS5

import os
import subprocess

import util.fs as fs
from util.printer import *
import util.ui as ui

# Check if we can call given arguments. Returns True if we can, False otherwise
def check_can_call(arglist):
    if len(arglist) > 0 and not subprocess.call(arglist, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) == 0:
        print('Cannot call "{0}" for some reason. Please check if you have access permission to it.'.format(arglist[0]))
        return False
    return True

# Tries to get Java version from commandline
def _get_shell_java_version(location='java'):
    check_can_call([location, '-version'])
    java_version = subprocess.check_output(location+' -version 2>&1 | awk -F[\\\"_] \'NR==1{print $2}\'', shell=True).decode('utf-8').split('.')
    tmp = int(java_version[1])
    return tmp if tmp > 1 else int(java_version[0])

# Tries to get Javac version from commandline
def _get_shell_javac_version(location='javac'):
    check_can_call([location, '-version'])
    javac_version = subprocess.check_output(location+' -version 2>&1 | awk -F\' \' \'{print $2}\'', shell=True).decode('utf-8').split('.')
    tmp = int(javac_version[1])
    return tmp if tmp > 1 else int(javac_version[0])

# Takes a path like /path/to/java-11-openjdk-11.0.3.../
# Returns Java major version if it follows standard pattern, otherwise 0
def _dirname_to_version(dirname):
    try:
        d = dirname.split(fs.sep())[-1] if fs.sep() in dirname else dirname
        v = d.split('-')[1]
        if '.' in v: # Version like 1.8.0
            return int(v.split('.')[1])
        else: # Version like 11
            return int(v)
    except Exception as e:
        return 0

# Gets shell-java's installation path.
# Note: May point to JRE instead of JDK!
def _get_shell_java_path():
    java_bin = subprocess.check_output('which java', shell=True).decode('utf-8').strip()
    if fs.issymlink(java_bin):
        java_bin = fs.resolvelink(java_bin)
    return java_bin

# Checks if the default 'java' command is valid java
# Returns full path to executable if we can use it, None otherwise
def _resolve1(minVersion, maxVersion):
    java_version = _get_shell_java_version()

    if java_version >= minVersion and java_version <= maxVersion:
        java_bin = _get_shell_java_path()
        if fs.dirname(fs.dirname(java_bin)).endswith('jre'): # JRE path found, jdk is commonly right above that path 
            java_bin = fs.join(fs.dirname(fs.dirname(fs.dirname(java_bin))), 'bin')
        if not 'javac' in fs.ls(java_bin, only_files=True):
            printw('Could not find valid JAVA_HOME using standard java.')
            return None
        return fs.dirname(java_bin) #from <path>/bin/ to <path>
    return None

'''
Find valid locations for Java home by walking from some path upwards,
scanning for directories containing the right names.

Yields found paths lazily
'''
def resolve2(minVersion, maxVersion, path=_get_shell_java_path()):
    p = str(path)
    dirlen = len(p.split(fs.sep()))
    for x in range(dirlen-1):
        p = fs.dirname(p)
        for item in fs.ls(p, only_dirs=True, full_paths=True):
            if fs.basename(item).startswith('java-') or 'openjdk' in fs.basename(item): #candidate found
                version = _dirname_to_version(fs.basename(item))
                if version < minVersion or version > maxVersion:
                    continue
                if (not fs.isdir(item, 'bin')) or (not fs.isfile(item, 'bin', 'java')) or not fs.isfile(item, 'bin', 'javac'):
                    continue
                yield item


def _write_bashrc(line, question=None, source_after=True):
    if question==None or ui.ask_bool(question):
        with open(fs.join(os.environ['HOME'], '.bashrc'), 'a') as file:
            file.write(line)
            if source_after:
                os.system('source ~/.bashrc')
        return True
    return False

# Checks if a Java installation within version bounds is available on the system
def check_version(minVersion=11, maxVersion=11):
    returncode = True
    if not 'JAVA_HOME' in os.environ:
        printw('JAVA_HOME is not set. Detecting automatically...')
        path = _resolve1(minVersion, maxVersion)
        exported = False
        if path != None:
            if _write_bashrc('export JAVA_HOME={}\n'.format(path), question='Export found location "{}" to your .bashrc?'.format(path)):
                exported = True
        if not exported:
            for path in resolve2(minVersion, maxVersion):
                print('Valid JAVA_HOME target found: {}'.format(path))
                if _write_bashrc('export JAVA_HOME={}\n'.format(path), question='Export found location "{}" to your .bashrc?'.format(path)):
                    exported = True
                    break
        if exported:
            return True
        else:
            printe('Unable to detect valid JAVA_HOME. Set JAVA_HOME yourself.')
            print('Note: Java is commonly installed in /usr/lib/jvm/...')
            return False
    elif not fs.isfile(os.environ['JAVA_HOME'], 'bin', 'java'):
        printe('Incorrect JAVA_HOME set: Cannot reach JAVA_HOME/bin/java ({0}/bin/java'.format(os.environ['JAVA_HOME']))
        print('Note: Java is commonly installed in /usr/lib/jvm/...')
        return False
    
    java_version_number = _get_shell_java_version(fs.join(os.environ['JAVA_HOME'], 'bin', 'java'))
    if java_version_number > maxVersion:
        printe('Your Java version is too new ({}). Please install Java version [{}-{}]. If you believe you have such version, use set_java.sh and set your JAVA_HOME to this correct version.'.format(java_version_number, minVersion, maxVersion))
        returncode = False
    elif java_version_number < minVersion:
        printe('Your Java version is too old ({}). Please install Java version [{}-{}]. If you believe you have such version, use set_java.sh and set your JAVA_HOME to this correct version'.format(java_version_number, minVersion, maxVersion))
        returncode = False

    
    javac_version_number = _get_shell_javac_version(fs.join(os.environ['JAVA_HOME'], 'bin', 'javac'))
    if javac_version_number > maxVersion:
        printe('Your Javac version is too new. Please install Java version [{0}-{1}]. If you believe you have such version, use set_javac.sh.'.format(minVersion, maxVersion))
        returncode = False
    elif javac_version_number < minVersion:
        printe('Your Javac version is too old. Please install Java version [{0}-{1}]. If you believe you have the right version, use set_javac.sh.'.format(minVersion, maxVersion))
        returncode = False

    return returncode