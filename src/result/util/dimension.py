class Dimension(object):
    @staticmethod
    def open_var(node, partitions_per_node, extension, compression, amount, kind, rb):
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
    def open_vars(node, partitions_per_node, extension, compression, amount, kind, rb):
        ans = []
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
    def num_open_vars(node, partitions_per_node, extension, compression, amount, kind, rb):
        return [node, partitions_per_node, extension, compression, amount, kind, rb].count(None)

    @staticmethod
    def make_id_string(frame, node, partitions_per_node, extension, compression, amount, kind, rb):
        ovars = Dimension.open_vars(node, partitions_per_node, extension, compression, amount, kind, rb)
        return ', '.join(['{}={}'.format(ovar, getattr(frame, str(ovar))) for ovar in ovars])

    @staticmethod
    def _open_var_numeric(name):
        return name == 'node' or name == 'partitions_per_node' or name == 'amount' or name == 'rb'

    @staticmethod
    def _make_axis_description(name):
        if name == 'node':
            return 'Executor cluster size (nodes)'
        if name == 'partitions_per_node':
            return 'Partitions per node'
        if name == 'extension':
            return 'Used filetype'
        if name == 'compression':
            return 'Used compression format'
        if name == 'amount':
            return 'data read [entries]'
        if name == 'kind':
            return 'Spark interaction type'
        if name == 'rb':
            return 'Readbuffer size [bytes]'
        raise RuntimeError('Could not get axis axis_description for "{}"'.format(name))

    class OpenVariable(object):
        def __init__(self, name, is_numeric=None, axis_description=None):
            self.name = name
            self.is_numeric = Dimension._open_var_numeric(name) if is_numeric == None else is_numeric
            self.axis_description = Dimension._make_axis_description(name) if axis_description == None else axis_description

        def val_to_ticks(self, val):
            if self.is_numeric and val > 1000:
                return '{:.2e}'.format(val)
            return str(val)

        def __str__(self):
            return self.name

        def __repr__(self):
            return self.name