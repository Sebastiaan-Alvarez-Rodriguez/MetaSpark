import subprocess
import remote.util.ip as ip

class Deployment(object):
    '''Object to contain, save and load node allocations'''
    def __init__(self, nodes=None, reservation_number=None, infiniband=True):
        if nodes == None and reservation_number==None:
            raise RuntimeError('Need either nodes or a reservation number to instantiate')
        elif nodes != None:
            self.nodes = nodes
        else:
            raw_nodes = subprocess.check_output("preserve -llist | grep "+str(reservation_number)+" | awk -F'\\t' '{ print $NF }'", shell=True).decode('utf-8').strip().split()
            self.nodes = [ip.node_to_infiniband_ip(int(x[4:])) for x in raw_nodes] if infiniband else raw_nodes

  
    # Save deployment to disk
    def persist(self, file):
        for x in self.nodes:
            file.write(x+'\n')

    # Load deployment from disk
    @staticmethod
    def load(file):
        return Deployment(nodes=[x.strip() for x in file.readlines()])