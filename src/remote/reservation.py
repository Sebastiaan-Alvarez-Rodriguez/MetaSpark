# This file contains code to make a reservation.
# Credits and thanks to professor Uta for the Bash scripts we use here!
import subprocess
import time

from remote.deployment import Deployment
from util.printer import *

class Reserver(object):
    '''Object to handle reservations on DAS5'''
    def __init__(self):
        self._reservation_number = None
        self._deployment = None

    @property
    def deployment(self):
        if self._reservation_number == None:
            raise RuntimeError('No reservation for this object!')
        if self._deployment == None:
            self._deployment = Deployment(reservation_number=self._reservation_number)
        return self._deployment

    # Reserve on DAS5 using given parameters
    def reserve(self, hosts, time_to_reserve, wait=True):
        reserve_command = 'preserve -np {} -1 -t {}'.format(hosts, time_to_reserve)
        reserve_command += " | grep \"Reservation number\" | awk '{ print $3 }' | sed 's/://'"
        self._reservation_number = int(subprocess.check_output(reserve_command, shell=True).decode('utf-8'))
        print('Made reservation {}'.format(self._reservation_number))
        if wait:
            print_start = True
            while not self._reservation_active():
                if print_start:
                    print('Waiting for reservation...')
                    print_start
                time.sleep(1)
            prints('Reservation ready!')
        

    # Returns True if reservation with given number is active, False otherwise
    def _reservation_active(self):
        if self._reservation_number == None:
            raise RuntimeError('Cannot check if reservation is active without reservation number!')
        return subprocess.check_output("preserve -llist | grep "+str(self._reservation_number)+" | awk '{ print $9 }'", shell=True).decode('utf-8').strip().startswith('node')

    # Store information about this object on disk, so we can load it later
    def persist(self):
        if self._reservation_number == None:
            raise RuntimeError('Nothing to persist: Missing reservation number!')
        with open(fs.join(fs.abspath(), 'reservation'), 'w') as file:
            file.write(self._reservation_number+'\n')
            self.deployment.persist(file)

    @staticmethod
    def load():
        with open(fs.join(fs.abspath(), 'reservation'), 'r') as file:
            r = Reserver()
            r.reservation_number = int(file.readline())
            r._deployment = Deployment.load(file)