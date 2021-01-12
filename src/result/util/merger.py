import util.fs as fs

def find_names():
    pass

def _filter_files(node=None, partitions_per_node=None, extension=None, amount=None, kind=None, rb=None):
    files = []
    for dnode in sorted(fs.ls(path, only_dirs=True), key=lambda x: int(x)):
        for dpartitions_per_node in sorted(fs.ls(fs.join(path, dnode), only_dirs=True), key=lambda x: int(x)):
            for dextension in fs.ls(fs.join(path, dnode, dpartitions_per_node), only_dirs=True):
                for damount in sorted(fs.ls(fs.join(path, dnode, dpartitions_per_node, dextension), only_dirs=True), key=lambda x: int(x)):
                    for dkind in fs.ls(fs.join(path, dnode, dpartitions_per_node, dextension, damount), only_dirs=True):
                        for outfile in sorted([x for x in fs.ls(fs.join(path, dnode, dpartitions_per_node, dextension, damount, dkind), only_files=True, full_paths=True) if x.endswith('.res')], key=lambda x: filename_to_rb(x)):
                            frb = filename_to_rb(outfile)
                            
                            files.append(outfile)
    print('Matched {} files'.format(len(files)))



def merge(resultpath, remove_first_lines=True):
    pre_remove_num = 2 if remove_first_lines else 0
    reader = Reader(resultpath)
