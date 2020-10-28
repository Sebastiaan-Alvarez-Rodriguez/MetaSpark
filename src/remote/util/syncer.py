import socket
import time

import util.fs as fs
import util.location as loc

from util.printer import *


class Syncer(object):
    '''
    Object to  handle synchronisation of all nodes between runs.
    Works with a barrier-style lock:
    Server 0 (prime) is the barrier lock holder and guide.
    All other servers and clients connect to prime and send a few bytes
    to notify prime they are waiting.
    Once prime has received notifications from all other nodes,
    it sends a few bytes back, signalling them to continue.
    This sending back is very fast, since the connections have remained open
    in the meantime.

    Initial measurements show this synchronisation strategy can sync
    all nodes to microsecond-length windows
    '''
    def __init__(self, config, experiment, designation, debug_mode=False):
        retries = 5 # Number of retries before we blame the network
        self.gid = config.gid
        self.designation = designation
        self.debug_mode = debug_mode

        self.prime = self.gid == 0 and self.designation == 'server'
        # Server 0 opens socket to listen
        if self.prime:
            self.serversock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.port = 2000
            connected = False
            while not connected:
                serveraddr = (socket.gethostname(), self.port)
                for x in range(retries):
                    try:
                        self.serversock.bind(serveraddr)
                        connected = True
                        break
                    except OSError as e:
                        if e.errno == 98: # Port is in use, try next port
                            self.port += 1
                            break
                        if x == 0:
                            printw('[SYNC] PRIME {}.{} cannot host from address {} (connection refused). Retrying...'.format(designation.upper(), self.gid, serveraddr))
                        elif x == retries-1:
                            raise e
            prints('Prime hosting from {}:{}'.format(socket.gethostname(), self.port))
            with open(fs.join(loc.get_metaspark_experiment_dir(), '.port.txt'), 'w') as file:
                file.write(str(self.port))
            self.serversock.listen(1070) #Get up to 1070 connections before refusing them
            self.expected_connections = experiment.num_servers-1+experiment.num_clients
            self.connections = []
            if self.debug_mode: print('PRIME stage 0! Address in use: {}'.format(serveraddr), flush=True)
            for x in range(self.expected_connections):
                connection, address = self.serversock.accept()
                self.connections.append(connection)
            if self.debug_mode: print('PRIME Got all {} connections'.format(self.expected_connections), flush=True)
        else: #Others open a socket to prime
            while not fs.isfile(loc.get_metaspark_experiment_dir(), '.port.txt'): # Wait until prime tells us which port to use
                time.sleep(5)
            with open(fs.join(loc.get_metaspark_experiment_dir(), '.port.txt'), 'r') as file:
                while True:
                    try:
                        self.port = int(file.readlines()[0])
                        break
                    except Exception as e:
                        time.sleep(1)
                        pass
            if self.designation == 'client':
                addr = (config.hosts[0].split(':')[0], self.port)
            else:
                addr = ('node{:03d}'.format(config.nodes[0]), self.port)
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.debug_mode: print('{}.{} CONNECTING TO addr: {}'.format(self.designation, self.gid, addr), flush=True)
            for x in range(retries):
                try:
                    self.sock.connect(addr)
                    break
                except ConnectionRefusedError as e:
                    if x == 0:
                        printw('[SYNC] {}.{} cannot connect to address {} (connection refused). Retrying...'.format(designation.upper(), self.gid, addr))
                    elif x == retries-1:
                        raise e
                    time.sleep(1)

    # Handle syncinc if we are prime server
    def _handle_sync_prime(self):
        if self.debug_mode: print('SYNC stage 1!', flush=True)
        for idx, conn in enumerate(self.connections):
            msg = conn.recv(2)
    
        if self.debug_mode: print('SYNC stage 2!', flush=True)
        # When arriving here, all expected servers and clients are connected and waiting for a reply
        for conn in self.connections:
            conn.sendall(b'go')

        if self.debug_mode: print('SYNC completed!', flush=True)

    # Handle syncing if we are not prime server
    def _handle_sync_other(self):
        try:
            self.sock.sendall(b'go')
            msg = self.sock.recv(2)
        except Exception as e:
            self.sock.close()
            raise e            

    # Synchronise with all other nodes
    def sync(self):
        if self.prime:
            self._handle_sync_prime()
        else:
            self._handle_sync_other()


    # Close network. Every node should call this to clean up
    def close(self):
        if self.prime:
            self.serversock.close()
            # Quickly close connections and be done with it
            for conn in self.connections:
                conn.close()
            fs.rm(loc.get_metaspark_experiment_dir(), '.port.txt')
        else:
            self.sock.close()