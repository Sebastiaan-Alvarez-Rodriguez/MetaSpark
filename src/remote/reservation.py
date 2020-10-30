# This file contains code to make a reservation.
# Credits and thanks to professor Uta for the Bash scripts we use here!
import subprocess
import time

class Reserver(object):
    '''Object to handle reservations on DAS5'''
    def __init__(self):
        self.number = None
        self.nodes = None

    # Reserve on DAS5 using given parameters
    def reserve(self, hosts, process_affinity, time_to_reserve, wait=True):
        reserve_command = 'preserve -np {} -{} -t {}'.format(hosts, affinity, time_to_reserve)
        reserve_command += " | grep \"Reservation number\" | awk '{ print $3 }' | sed 's/://'"
        self.reservation_number = int(subprocess.check_output(reserve_command, shell=True).decode('utf-8'))
        print('Made reservation {}'.format(reservation_number))
        if wait:
            print_start = True
            while not self._reservation_active():
                if print_start:
                    print('Waiting for reservation...')
                    print_start
                time.sleep(1)
            prints('Reservation ready!')
        

    # Returns a list of reserved nodes
    def get_reserved_nodes(self, infiniband=False):
        if self.reservation_number == None:
            raise RuntimeError('Cannot get reserved nodes if no reservation has been done!')
        if self.nodes == None:
            self.nodes = subprocess.check_output("preserve -llist | grep "+str(self.reservation_number)+" | awk -F'\\t' '{ print $NF }'").check_output('utf-8').strip().split()
        if infiniband:
            return [ip.node_to_infiniband_ip(int(x[4:])) for x in self.nodes]
        else:
            return self.nodes


    # Returns True if reservation with given number is active, False otherwise
    def _reservation_active(self):
        if self.reservation_number == None:
            raise RuntimeError('Cannot check if reservation is active without reservation number!')
        return subprocess.check_output("preserve -llist | grep "+str(self.reservation_number)+" | awk '{ print $9 }'").check_output('utf-8').strip().startswith('node')
