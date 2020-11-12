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
            raw_nodes = subprocess.check_output("preserve -llist | grep "+str(reservation_number)+" | awk -F'\\t' '{ print $NF }'", shell=True).decode('utf-8').strip().split()
            raw_nodes.sort(key=lambda x: int(x[4:]))
            self._nodes = [ip.node_to_infiniband_ip(int(x[4:])) for x in raw_nodes] if infiniband else raw_nodes

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

    # Save deployment to disk
    def persist(self, file):
        file.write(str(self._master_port)+'\n')
        for x in self._nodes:
            file.write(x+'\n')

    # Load deployment from disk
    @staticmethod
    def load(file):
        port = int(file.readline().strip())
        deployment = Deployment()
        deployment._nodes = [x.strip() for x in file.readlines()]
        deployment.master_port = port
        return deployment