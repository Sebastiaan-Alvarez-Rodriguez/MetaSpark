import os
import re
import subprocess

from util.printer import *

# Finds a java process with enabled flightrecorder matching regex
# Returns integer PID if found, None otherwise
def find_proc_regex(regex='([0-9]+) .*ExecutorBackend'):
    out = subprocess.check_output('$JAVA_HOME/bin/jps', shell=True).decode('utf-8').strip()
    res = re.search(regex, out)

    # if res != None:
    #     import socket
    #     ans = int(res.group(1))
    #     print('{} found that the following contains a good entry:\n{}\nAnswer: {}'.format(socket.gethostname(), out, ans))
    return int(res.group(1)) if res != None else None

# Launch flightrecording for given pid, for specified duration, with specified delay before recording.
# Default delay is 0 seconds (s). Can also specify minutes (m) or hours (h).
# Default duration is in seconds (s). Can also specify minutes (m) or hours (h).
# Returns directly after starting the flightrecording.
# Outputs a .jfr file on given absolute path, after specified delay+duration has passed.
def launch_flightrecord(pid, abs_path, duration='30s', delay='0s'):
    command = '$JAVA_HOME/bin/jcmd {} JFR.start'.format(pid)
    command += ' delay={}'.format(delay) if delay != '0s' else ''
    command += ' duration={} filename={} settings=profile'.format(duration, abs_path)
    os.system(command)