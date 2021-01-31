# This file contains code to make a reservation.
# Credits and thanks to professor Uta for the Bash scripts we use here!
import subprocess
import time

from remote.deployment import Deployment
import util.fs as fs
import util.location as loc
from util.printer import *

from threading import Lock

class ReservationExpiredError(Exception):
    pass


class Reservation(object):
    def __init__(self, reservation_number, deployment):
        self.number = reservation_number
        self.deployment = deployment

    def validate(self):
        return Reservation.check_active(self.number)

    # Store information about this object on disk, so we can load it later
    def persist(self):
        if self.number == None:
            raise RuntimeError('Nothing to persist: Missing reservation number!')
        with open(fs.join(loc.get_metaspark_reservations_dir(), 'reservation.{}'.format(self.number)), 'w') as file:
            file.write(str(self.number)+'\n')
            self.deployment.persist(file)


    # Returns True if reservation exists, False otherwise
    @staticmethod
    def check_active(reservation_number):
        if reservation_number == None:
            raise RuntimeError('Cannot check if reservation is active without reservation number!')
        return subprocess.check_output("preserve -llist | grep "+str(reservation_number)+" | awk '{ print $9 }'", shell=True).decode('utf-8').strip().startswith('node')


    # Load reservation from disk, if it exists.
    # Raises FileNotFoundError if there is no reservation file
    # Raises ReservationExpiredError if reservation has expired
    # Returns the reservation
    @staticmethod
    def load_from(path):
        if not fs.isfile(path):
            raise FileNotFoundError('No reservation found')
        with open(path, 'r') as file:
            number = int(file.readline().strip())
            if not Reservation.check_active(number):
                fs.rm(path)
                raise ReservationExpiredError('Reservation found, but no longer active!')
            deployment = Deployment.load(file)
            return Reservation(number, deployment=deployment)


class ReservationManager(object):
    '''Object responsible for creating/deleting (managing) reservations'''

    def __init__(self):
        print('Initializing Reservation manager...')
        self.lock = Lock()
        self.reservations = dict() # Mapping from reservation number to Reservation object
        
        fs.mkdir(loc.get_metaspark_reservations_dir(), exist_ok=True)
        expired = 0
        for filepath in fs.ls(loc.get_metaspark_reservations_dir(), only_files=True, full_paths=True):
            basepath = fs.basename(filepath)
            if basepath.startswith('reservation.'):
                try:
                    self.reservations[int(basepath.split('.', 1)[1])] = Reservation.load_from(filepath)
                except ReservationExpiredError as e:
                    expired += 1
                except Exception as e:
                    printw('Error for file {}: {}'.format(filepath, e))
        prints('Loaded {} reservation{} ({} expired)'.format(len(self.reservations), 's' if len(self.reservations) != 1 else '', expired))


    # Gets reservation for given reservation number. 
    # By default, checks whether reservation still is active.
    # If no longer active, returns None. Returns reservation with matching number otherwise
    def get(self, number, check=True):
        with self.lock:
            reservation = self.reservations[number]
            if check and not Reservation.check_active(number):
                return None
            return reservation


    # Reserve on DAS5 using given parameters. Blocks until reservation is ready to be used
    def reserve(self, hosts, time_to_reserve, infiniband):
        reserve_command = 'preserve -np {} -1 -t {}'.format(hosts, time_to_reserve)
        reserve_command += " | grep \"Reservation number\" | awk '{ print $3 }' | sed 's/://'"
        number = int(subprocess.check_output(reserve_command, shell=True).decode('utf-8'))
        print('Made reservation {}'.format(number))    
        while not Reservation.check_active(number):
            print('Waiting for reservation...')
            time.sleep(1)
        prints('Reservation ready!')
        r = Reservation(number,  Deployment(reservation_number=number, infiniband=infiniband))
        r.persist()
        with self.lock:
            self.reservations[number] = r
        return r

    # Update reservation when new information becomes known. Useful when we want to store some new info
    def update_reservation(self,reservation, number=None):
        if number == None:
            number = reservation.number
        with self.lock:
            self.reservations[number] = reservation
        reservation.persist()

    # Stops a running reservation on DAS5
    def stop(self, item, no_locking=False):
        if isinstance(item, int):
            number = item
        elif isinstance(item, Reservation):
            number = item.number
        else:
            raise RuntimeError('I have no idea how to stop a reservation with given param "{}"'.format(item))
        out = subprocess.call('preserve -c {}'.format(number), shell=True)
        retval = out == 0 or out == 33 # Code 33 means "reservation not found". That is just fine, we wanted it gone.
        if not retval:
            printe('Error while stopping cluster with id {} (output was {})'.format(number, out))
        else:
            fs.rm(fs.join(loc.get_metaspark_reservations_dir(), 'reservation.{}'.format(number)))
            if no_locking:
                del self.reservations[number]
            else:
                with self.lock:
                    del self.reservations[number]
        return retval


    def stop_all(self):
        with self.lock:
            for val in list(self.reservations.values()):
                self.stop(val, no_locking=True) # We already own the lock


    def stop_selected(self, selected):
        with self.lock:
            for x in selected:
                self.stop(x, no_locking=True) # We already own the lock


# Import this if you wish to use the manager
reservation_manager = ReservationManager()