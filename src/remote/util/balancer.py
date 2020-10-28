# The simplest load balancer available.
# Maps every next client to the next server, wrapping around the end of the server list
# Returns the target for given gid from the server list
def balance_simple(serverlist, gid):
    return serverlist[gid % len(serverlist)]
