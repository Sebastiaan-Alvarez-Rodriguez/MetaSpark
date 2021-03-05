import sys
import subprocess

def _get_version(packagename):
    if sys.version_info >= (3, 8):
        from importlib.metadata import version
        return version(packagename)
    else:
        return subprocess.check_output('pip freeze | grep {} | awk -F\'=\' \'{print $NF}\''.format(packagename)).decode('UTF-8').strip()

# Returns `True` if given version array (e.g. [0,9,9,2] for '0.9.9.2') is equal to given version string.
# By default, `greater_allowed==True`, which allows version strings with a higher version to be accepted.
def _ensure_version(nums, string, greater_allowed=True):
    for idx, x in enumerate(nums):
        try:
            found, string = string.split('.', 1)
        except ValueError as e: # Previous version numbers equal to minimum, now out of numbers
            return not any(x for x in nums[idx:] if x > 0) # Return True if minimum has only zeroes from here
        
        if int(found) > x: # Current version number larger than minimum required
            return True and greater_allowed
        elif int(found) < x: # Previous version numbers equal to minimum, now smaller
            return False


# Geni needs the `six` package installed, but does not specify this in manifest. Therefore, We check this.
# Returns `True` if `six` installed, `False` otherwise.
def geni_six_check():
    try:
        import six
        return True
    except Exception as e:
        return False


# Geni uses `lxml` package. `lxml` needs python>=3.5 or python==2.7.
def geni_lxml_check():
    try:
        import lxml
        return True
    except Exception as e:
        return False


# Checks all dependencies at once. Returns `True` when all dependencies are satisfied.
# Optionally Stops checking when 1 depende
def geni_dependency_checks(stop_on_error=False):
    val = True
    for x in (geni_six_check, geni_lxml_check):
        val &= x()
        if not val and stop_on_error:
            return False
    return val


# Returns `True` if geni-lib==0.9.9.2 is installed, with all needed dependencies, `False` otherwise.
# Optionally turn off checking dependencies with the `check_deps` param.
def geni_check(check_deps=True):
    try:
        import geni
        val = _ensure_version([0,9,9,2], _get_version('geni-lib'), greater_allowed=False)
        if check_deps and val:
            val &= geni_dependency_checks()
        return val
    except Exception as e:
        return False