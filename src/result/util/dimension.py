class Dimension(object):
    @staticmethod
    def open_var(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb):
        if num_cols == None:
            return Dimension.OpenVariable('num_cols')
        if compute_cols == None:
            return Dimension.OpenVariable('compute_cols')
        if node == None:
            return Dimension.OpenVariable('node')
        if partitions_per_node == None:
            return Dimension.OpenVariable('partitions_per_node')
        if extension == None:
            return Dimension.OpenVariable('extension')
        if compression == None:
            ans.append(Dimension.OpenVariable('compression'))
        if amount == None:
            return Dimension.OpenVariable('amount')
        if kind == None:
            return Dimension.OpenVariable('kind')
        if rb == None:
            return Dimension.OpenVariable('rb')
        raise RuntimeError('No open vars found')

    @staticmethod
    def open_vars(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb):
        ans = []
        if num_cols == None:
            ans.append(Dimension.OpenVariable('num_cols'))
        if compute_cols == None:
            ans.append(Dimension.OpenVariable('compute_cols'))
        if node == None:
            ans.append(Dimension.OpenVariable('node'))
        if partitions_per_node == None:
            ans.append(Dimension.OpenVariable('partitions_per_node'))
        if extension == None:
            ans.append(Dimension.OpenVariable('extension'))
        if compression == None:
            ans.append(Dimension.OpenVariable('compression'))
        if amount == None:
            ans.append(Dimension.OpenVariable('amount'))
        if kind == None:
            ans.append(Dimension.OpenVariable('kind'))
        if rb == None:
            ans.append(Dimension.OpenVariable('rb'))
        return ans


    @staticmethod
    def num_open_vars(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb):
        return [num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb].count(None)

    @staticmethod
    def make_id_string(frame, num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb):
        ovars = Dimension.open_vars(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb)
        return ', '.join(['{}={}'.format(ovar, getattr(frame, str(ovar))) for ovar in ovars])

    @staticmethod
    def open_var_numeric(name):
        return name == 'num_cols' or name == 'compute_cols' or name == 'node' or name == 'partitions_per_node' or name == 'amount' or name == 'rb'

    @staticmethod
    def _make_axis_description(name):
        if name == 'num_cols':
            return 'Total columns'
        if name == 'compute_cols':
            return 'Used columns'
        if name == 'node':
            return 'Executor cluster size (nodes)'
        if name == 'partitions_per_node':
            return 'Partitions per node'
        if name == 'extension':
            return 'Used filetype'
        if name == 'compression':
            return 'Used compression format'
        if name == 'amount':
            return 'data read [rows]'
        if name == 'kind':
            return 'Spark data representation type'
        if name == 'rb':
            return 'Batch size [rows]'
        raise RuntimeError('Could not get axis axis_description for "{}"'.format(name))


    class OpenVariable(object):
        def __init__(self, name, is_numeric=None, axis_description=None):
            self.name = name
            self.is_numeric = Dimension.open_var_numeric(name) if is_numeric == None else is_numeric
            self.axis_description = Dimension._make_axis_description(name) if axis_description == None else axis_description

        def val_to_ticks(self, val):
            if self.name=='amount' and val > 1000:
                return '{:.2e}'.format(val)
            return str(val)

        def __str__(self):
            return self.name

        def __repr__(self):
            return self.name