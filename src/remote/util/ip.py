import os

# Execute on remote or node level to get infiniband ip address for given node number
def node_to_infiniband_ip(node_nr):
     return '10.149.'+('{:03d}'.format(node_nr)[0])+'.'+'{}'.format(int(node_nr)%100)

# Converts given ip/node to its infiniband IP on the DAS5
def infiniband_ip_to_node(ip):
    return 'node{:03d}'.format(int(ip.split('.')[-1])) if is_infiniband(ip) else ip

# Returns True if the given ip/node turns out to be an infiniband ip on DAS5, False otherwise
def is_infiniband(ip):
    return ip.startswith('10.149.')

# Returns the address to the prime node
def master_address(use_infiniband):
    # node117
    nodename = os.environ['HOSTS'].split()[0]
    return node_to_infiniband_ip(int(nodename[4:])) if use_infiniband else nodename