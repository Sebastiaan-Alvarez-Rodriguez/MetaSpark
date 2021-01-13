import socket
import subprocess
import remote.util.ip as ip
class Deployment(object):
    '''Object to contain, save and load node allocations'''

    '''
    master_port:        Master port to report when asking for master_port/master_url properties.
    reservation_number: Optional int. If set, fetches node names and builds "nodes" property.
    infiniband:         Return whether to convert ips to infiniband. Does nothing without reservation_number set.
    '''
    def __init__(self, master_port=7077, reservation_number=None, infiniband=True):
        self._nodes = None
        self._master_port = None
        if reservation_number != None:
            self.raw_nodes = subprocess.check_output("preserve -llist | grep "+str(reservation_number)+" | awk -F'\\t' '{ print $NF }'", shell=True).decode('utf-8').strip().split()
            self.raw_nodes.sort(key=lambda x: int(x[4:]))
            self._nodes = [ip.node_to_infiniband_ip(int(x[4:])) for x in self.raw_nodes] if infiniband else self.raw_nodes
        self.infiniband = infiniband

    @property
    def nodes(self):
        return self._nodes

    @property
    def master_ip(self):
        return self._nodes[0]

    @property
    def master_port(self):
        return self._master_port

    @master_port.setter
    def master_port(self, val):
        self._master_port = int(val)

    @property
    def master_url(self):
        return 'spark://{}:{}'.format(self.master_ip, self._master_port)

    @property
    def slave_ips(self):
        return self._nodes[1:]

    # Returns whether this host is the master node
    def is_master(self):
        return self.raw_nodes[0] == socket.gethostname()

    # Returns the global id of this host
    def get_gid(self):
        return self.raw_nodes.index(socket.gethostname())

    # Save deployment to disk
    def persist(self, file):
        file.write(str(self._master_port)+'\n')
        file.write(str(self.infiniband)+'\n')
        for x in self.raw_nodes:
            file.write(x+'\n')

    # Load deployment from disk
    @staticmethod
    def load(file):
        deployment = Deployment()
        deployment.master_port = int(file.readline().strip())
        deployment.infiniband = file.readline().strip()=='True'
        deployment.raw_nodes = [x.strip() for x in file.readlines()]
        deployment._nodes = [ip.node_to_infiniband_ip(int(x[4:])) for x in deployment.raw_nodes] if deployment.infiniband else deployment.raw_nodes
        return deployment