# Contains functions to interact with Python's import libraries.
# As there import libraries change a lot between versions,
# this file is essential to work with importlib.

import sys
import importlib

import util.fs as fs

# Check if a given library exists. Returns True if given name is a library, False otherwise
def library_exists(name):
    if sys.version_info >= (3, 4):
        return importlib.util.find_spec(str(name)) is not None
    else:
        raise NotImplementedError('Did not implement existence check for Python 3.3 and below')

# Import a library from a filesystem full path (i.e. starting from root)
def import_full_path(full_path):
    module_name = '.'.join(full_path.split(fs.sep()))
    if sys.version_info >= (3, 5):
        import importlib.util
        spec = importlib.util.spec_from_file_location(module_name, full_path)
        foo = importlib.util.module_from_spec(spec)
        return spec.loader.exec_module(foo)
    elif sys.version_info >= (3, 3):
        from importlib.machinery import SourceFileLoader
        return SourceFileLoader(module_name, full_path).load_module()
    elif sys.version_info <= (2, 9):
        import imp
        return imp.load_source(module_name, full_path)
    else:
        raise NotImplementedError('Did not implement existence check for Python >2.9 and <3.3')