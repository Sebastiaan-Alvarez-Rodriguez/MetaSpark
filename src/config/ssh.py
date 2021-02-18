# This file contains code to generate small SSH config files,
# containing SSH options.


import configparser

import util.fs as fs
from util.printer import *
import util.ui as ui


def get_metaspark_conf_dir():
    return fs.join(fs.abspath(), 'conf')

def get_metaspark_ssh_conf_dir():
    return fs.join(get_metaspark_conf_dir(), 'ssh')

def ask_ssh_key_name():
    return ui.ask_string('Please type the name of your SSH entry (e.g. "das5LU")', confirm=True)

def ask_ssh_user_name():
    return ui.ask_string('What is your username on the remote machine (e.g. "dasuser42")?')

def ask_ssh_available_nodes():
    return ui.ask_int('How many nodes does this machine have?',minval=1)

def ask_remote_metaspark_dir(default_dir=None):
    q = '''
In which directory to install metaspark on the remote machine (e.g. /some/path)?
Note: We place a directory named "MetaSpark" inside of the directory you pick here.
Note: This directory you choose MUST be accessible by all nodes we might spawn
on the remote. Recommendation: /var/scratch/<UserName>
'''
    if default_dir != None:
        val = ui.ask_string(q+'Leave empty for default {}...'.format(default), empty_ok=True)  
        return val if len(val > 0) else default_dir
    return ui.ask_string(q)


# Ask user questions to generate a config. Returns path to new config
def gen_config():
    keyname = ask_ssh_key_name()
    sshuser = ask_ssh_user_name()
    remotedir = ask_remote_metaspark_dir()
    total_nodes = ask_ssh_available_nodes()
    while True:
        configloc = fs.join(get_metaspark_ssh_conf_dir(), fs.basename(ui.ask_string('Please give a name to this configuration')))
        if not configloc.endswith('.cfg'):
            configloc += '.cfg'
        if (not fs.isfile(configloc)) or ui.ask_bool('Config "{}" already exists, override?').format(configloc):
            write_config(configloc, keyname, sshuser, remotedir, total_nodes)
            return configloc
        else:
            printw('Pick another configname.')


# Persist a configuration file
def write_config(configloc, key_name, user, metaspark_dir, total_nodes):
    fs.mkdir(get_metaspark_ssh_conf_dir(), exist_ok=True)
    parser = configparser.ConfigParser()
    parser['SSH'] = {
        'key_name': key_name,
        'user': user,
        'metaspark_dir': metaspark_dir,
        'total_nodes': total_nodes
    }
    with open(configloc, 'w') as file:
        parser.write(file)


# Change an amount of user settings
def change_settings():
    if (not fs.isdir(get_metaspark_ssh_conf_dir())) or fs.isemptydir(get_metaspark_ssh_conf_dir()):
        if ui.ask_bool('No SSH configs found. Make one?'):
            gen_config(get_metaspark_settings_file())
        return
    while True:
        cfg_paths = [x for x in fs.ls(get_metaspark_ssh_conf_dir(), only_files=True, full_paths=True) if x.endswith('.cfg')]
        idx = ask_pick('Which SSH config to change settings for?', [fs.basename(x) for x in cfg_paths])
        chosen = cfg_paths[idx]

        settings = SSHConfig(chosen)
        l = ['key_name', 'user', 'metaspark_dir', 'total_nodes']
        while True:
            idx = ui.ask_pick('Which setting to change?', l)
            cur = [settings.ssh_key_name, settings.ssh_user_name, settings.remote_metaspark_dir, settings.total_nodes]
            print('\nCurrent value: "{}"'.format(cur[idx]))
            if idx == 0:
                settings.ssh_key_name = ask_ssh_key_name()
            elif idx == 1:
                settings.ssh_user_name = ask_ssh_user_name()
            elif idx == 2:
                settings.remote_metaspark_dir = ask_remote_metaspark_dir()
            elif idx == 3:
                settings.total_nodes = ask_ssh_available_nodes()
            settings.persist()
            if ui.ask_bool('Done with this config?'):
                break
        if ui.ask_bool('Done with changing SSH settings?'):
            return


# Check if all required data is present in the config
def validate_settings(configloc):
    d = dict()
    d['SSH'] = {'key_name', 'user', 'metaspark_dir', 'total_nodes'}

    parser = configparser.ConfigParser()
    parser.optionxform=str
    parser.read(configloc)
    for key in d:
        if not key in parser:
            raise RuntimeError('Missing section "{}"'.format(key))
        else:
            for subkey in d[key]:
                if not subkey in parser[key]:
                    raise RuntimeError('Missing key "{}" in section "{}"'.format(subkey, key))


def get_configs():
    return [SSHConfig(x) for x in fs.ls(get_metaspark_ssh_conf_dir(), only_files=True) if x.endswith('.cfg')]


class SSHConfig(object):    
    '''
    Simple object to quickly interact with stored SSH settings.
    This way, we don't have to read in the config every time,
    or pass it along a large amount of times.
    Below, we define a global instance.
    '''
    def __init__(self, name):
        if not name.endswith('.cfg'):
            name += '.cfg'
        self.picked = fs.join(get_metaspark_ssh_conf_dir(), name)
        validate_settings(self.picked)
        self.parser = configparser.ConfigParser()
        self.parser.optionxform=str
        self.parser.read(self.picked)


    # SSH key to use when communicating with remote
    @property
    def ssh_key_name(self):
        return self.parser['SSH']['key_name']

    # Username on remote
    @property
    def ssh_user_name(self):
        return self.parser['SSH']['user']

    # Path to the desired location to store metaspark on the remote
    @property
    def remote_metaspark_dir(self):
        return self.parser['SSH']['metaspark_dir']

    # Total number of nodes on remote
    @property
    def total_nodes(self):
        return int(self.parser['SSH']['total_nodes'])

    # Path to this config
    @property
    def path(self):
        return self.picked

    # Persist current settings
    def persist():
        with open(self.picked, 'w') as file:
            parser.write(file)