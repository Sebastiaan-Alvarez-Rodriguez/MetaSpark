import getpass
import subprocess
'''File containing DAS-specific tricks to get info from the reservation system'''


# Returns True if we have a running reservation
def have_reservation(user=None):
    if user == None:
        user = getpass.getuser()
    return len(subprocess.check_output('preserve -llist | awk \'{ print $2,$7}\' | grep \'{} R\''.format(user), shell=True).decode('utf-8').strip()) > 0


# Returns number of currently used nodes.
# Note: We only register reservations in R (run) state during this computation.
def nodes_used():
    return int(subprocess.check_output('preserve -llist | awk \'{ print $7,$8,$9 }\' | grep R | awk \'{s+=$2}END{print s}\'', shell=True).decode('utf-8').strip())


# Returns string of nodes reserved for given reservation number
def nodes_for_reservation(reservation_number):
    return subprocess.check_output('preserve -llist | grep '+str(reservation_number)+' | awk \'{ print $9 }\'', shell=True).decode('utf-8').strip()


# Places reservation on DAS with num_nodes for given time.
# Returns the reservation number.
# Note: Reservation may take a while. You have to check whether reservation succeeded!
def reserve_nodes(num_nodes, time_to_reserve):
    reserve_command = 'preserve -np {} -1 -t {}'.format(num_nodes, time_to_reserve)
    reserve_command += ' | grep "Reservation number" | awk \'{ print $3 }\' | sed \'s/://\''
    return int(subprocess.check_output(reserve_command, shell=True).decode('utf-8'))


# Cancels reservation with given number.
# Returns the exit code of the preserve command.
# Note: Exit code 0 means success (of course).
#       Exit code 33 means that the reservation number was not found.
def reserve_cancel(reservation_number):
    return subprocess.call('preserve -c {}'.format(number), shell=True)