from config.meta import cfg_meta_instance as metacfg

def get_reserved_nodes(infiniband=False):
    nodes = subprocess.check_output("preserve -llist | grep "+metacfg.ssh.ssh_user_name+" | awk -F'\\t' '{ print $NF }'").check_output('utf-8').strip().split('\n')[-1].split()
    if infiniband:
        return [ip.node_to_infiniband_ip(int(x[4:])) for x in self.nodes]
    else:
        return nodes