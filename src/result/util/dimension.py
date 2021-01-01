class Dimension(object):
    @staticmethod
    def open_var(node, partitions_per_node, extension, amount, kind, rb):
        if node == None:
            return 'node'
        elif partitions_per_node == None:
            return 'partitions_per_node'
        elif extension == None:
            return 'extension'
        elif amount == None:
            return 'amount'
        elif kind == None:
            return 'kind'
        elif rb == None:
            return 'rb'
        raise RuntimeError('No open vars found')

    @staticmethod
    def open_vars(node, partitions_per_node, extension, amount, kind, rb):
        ans = []
        if node == None:
            ans.append('node')
        if partitions_per_node == None:
            ans.append('partitions_per_node')
        if extension == None:
            ans.append('extension')
        if amount == None:
            ans.append('amount')
        if kind == None:
            ans.append('kind')
        if rb == None:
            ans.append('rb')
        return ans

    @staticmethod
    def num_open_vars(node, partitions_per_node, extension, amount, kind, rb):
        return [node, partitions_per_node, extension, amount, kind, rb].count(None)

    @staticmethod
    def make_id_string(frame, node, partitions_per_node, extension, amount, kind, rb):
        ovars = Dimension.open_vars(node, partitions_per_node, extension, amount, kind, rb)
        return ', '.join(['{}={}'.format(ovar, getattr(frame, ovar)) for ovar in ovars])