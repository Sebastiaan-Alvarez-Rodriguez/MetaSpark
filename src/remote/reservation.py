# This file contains code to make a reservation.
# Credits and thanks to professor Uta for the Bash scripts we use here!
import subprocess
import time

from remote.deployment import Deployment
import util.fs as fs
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

    @property
    def reservation_number(self):
        return self._reservation_number
    
    # Reserve on DAS5 using given parameters
    def reserve(self, hosts, time_to_reserve, wait=True):
        reserve_command = 'preserve -np {} -1 -t {}'.format(hosts, time_to_reserve)
        reserve_command += " | grep \"Reservation number\" | awk '{ print $3 }' | sed 's/://'"
        self._reservation_number = int(subprocess.check_output(reserve_command, shell=True).decode('utf-8'))
        print('Made reservation {}'.format(self._reservation_number))
        if wait:
            print_start = True
            while not Reserver._reservation_active(self._reservation_number):
                if print_start:
                    print('Waiting for reservation...')
                    print_start
                time.sleep(1)
            prints('Reservation ready!')

    # Stops reservation
    def stop(self):
        if self._reservation_number == None:
            raise RuntimeError('Cannot stop reservation without reservation number')
        out = subprocess.call('preserve -c {}'.format(self.reservation_number), shell=True)
        retval = out == 0 or out == 33 # Code 33 is sometimes given (no documentation/manual on it). The cluster is deallocated just fine when that happens.
        if not retval:
            printe('Error while stopping cluster with id {} (output was {})'.format(self._reservation_number, out))
        else:
            fs.rm(fs.abspath(), 'reservation')
        return retval

    # Store information about this object on disk, so we can load it later
    def persist(self):
        if self._reservation_number == None:
            raise RuntimeError('Nothing to persist: Missing reservation number!')
        with open(fs.join(fs.abspath(), 'reservation'), 'w') as file:
            file.write(str(self._reservation_number)+'\n')
            self.deployment.persist(file)


    # Returns True if reservation exists, False otherwise
    @staticmethod
    def _reservation_active(reservation_number):
        if reservation_number == None:
            raise RuntimeError('Cannot check if reservation is active without reservation number!')
        return subprocess.check_output("preserve -llist | grep "+str(reservation_number)+" | awk '{ print $9 }'", shell=True).decode('utf-8').strip().startswith('node')


    # Load reservation from disk, if it exists.
    # Raises FileNotFoundError if there is no reservation file
    # Raises ReservationExpiredError if reservation has expired
    # Returns the reservation
    @staticmethod
    def load():
        if not fs.isfile(fs.abspath(), 'reservation'):
            raise FileNotFoundError('No reservation found')
        with open(fs.join(fs.abspath(), 'reservation'), 'r') as file:
            r = Reserver()
            r._reservation_number = int(file.readline().strip())
            if not Reserver._reservation_active(r.reservation_number):
                fs.rm(fs.abspath(), 'reservation')
                raise ReservationExpiredError('Reservation found, but no longer active!')
            r._deployment = Deployment.load(file)
            return r

class ReservationExpiredError(Exception):
    pass

