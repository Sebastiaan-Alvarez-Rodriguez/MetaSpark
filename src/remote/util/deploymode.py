from enum import Enum

class DeployMode(Enum):
    '''Possible deployment modes'''
    STANDARD = 0 # on NFS mount
    LOCAL = 1    # on /local/<user>/
    LOCAL_SSD = 2    # on /local-ssd/<user>/
    RAM = 3      # on /dev/shm or /run/shm

    @staticmethod
    def interpret(string):
        d = string.strip().lower()
        if d == 'standard':
            return DeployMode.STANDARD
        elif d == 'local':
            return DeployMode.LOCAL
        elif d == 'local_ssd' or d == 'local-ssd':
            return DeployMode.LOCAL_SSD
        elif d == 'ram' or d == 'ramdisk':
            return DeployMode.RAM
        else:
            raise RuntimeError('Could not determine DeployMode for user argument "{}"'.format(string))

    def __str__(self):
        return self.name.lower()