import os

# Execute on remote or node level to get infiniband ip address for given node number
def node_to_infiniband_ip(node_nr):
    return '10.149.'+('{:03d}'.format(node_nr)[0])+'.'+'{:03d}'.format(node_nr)[1:]


# Returns the address to the prime node
def master_address(use_infiniband):
    # node117
    nodename = os.environ['HOSTS'].split()[0]
    return node_to_infiniband_ip(int(nodename[4:])) if use_infiniband else nodename