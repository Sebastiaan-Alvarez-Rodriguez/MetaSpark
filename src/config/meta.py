# This file contains code to generate a small config file,
# containing the only stateful data of MetaSpark.
# 
# The data stored here should be things we do not want to ask
# the user every time they use MetaSpark


import configparser

import util.fs as fs
from util.printer import *
import util.ui as ui
import config.ssh as ssh        

def get_metaspark_conf_dir():
    return fs.join(fs.abspath(), 'conf')

def get_metaspark_ssh_conf_dir():
    return fs.join(get_metaspark_conf_dir(), 'ssh')

# Gets path to config storage location
def get_metaspark_metaconf_file():
    return fs.join(get_metaspark_conf_dir(), 'metaspark.cfg')


# Ask user to pick a ssh config, returns full path to chosen config
def ask_ssh_config_name():
    if (not fs.isdir(get_metaspark_ssh_conf_dir())) or fs.isemptydir(get_metaspark_ssh_conf_dir()):
        return ssh.gen_config()

    cfg_paths = [x for x in fs.ls(get_metaspark_ssh_conf_dir(), only_files=True, full_paths=True) if x.endswith('.cfg')]
    idx = ui.ask_pick('Which SSH config to use?', [fs.basename(x) for x in cfg_paths])
    return cfg_paths[idx]  


# Ask user questions to generate a config
def gen_config(configloc):
    write_config(configloc, fs.basename(ask_ssh_config_name()[:-4]))


# Persist a configuration file
def write_config(configloc, sshconfig):
    parser = configparser.ConfigParser()
    parser['Meta'] = {
        'ssh_config_name': sshconfig
    }
    with open(configloc, 'w') as file:
        parser.write(file)

# Change user settings for meta config
def _change_ssh_config():
    cfg_meta_instance.ssh_config_name = fs.basename(ask_ssh_config_name()[:-4])
    cfg_meta_instance.persist()

# Change an amount of user settings
def change_settings():
    if not fs.exists(get_metaspark_metaconf_file()):
        gen_config(get_metaspark_metaconf_file())
        return
    opts = ['Change used SSH config', 'Go to SSH config settings', 'Back']
    idx = ui.ask_pick('Which setting to change?', opts)
    if idx == 0:
        _change_self_settings()
    elif idx == 1:
        ssh.change_settings()
    else:
        return

# Check if all required data is present in the config
def validate_settings(configloc):
    d = dict()
    d['Meta'] = {'ssh_config_name'}
    
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


class MetaConfig(object):    
    '''
    Persist project-level stateful information to disk,
    so we don't have to ask user for everything 
    every time they use MetaSpark.
    '''
    def __init__(self):
        loc = get_metaspark_metaconf_file()
        if not fs.exists(loc):
            gen_config(loc)
        else:
            validate_settings(loc)
        self.parser = configparser.ConfigParser()
        self.parser.optionxform=str
        self.parser.read(loc)

        self._ssh = ssh.SSHConfig(self.ssh_config_name)

    # SSH config to load
    @property
    def ssh_config_name(self):
        return self.parser['Meta']['ssh_config_name']

    @ssh_config_name.setter
    def ssh_config_name(self, val):
        self._ssh = ssh.SSHConfig(self.val)

    @property
    def ssh(self):
        return self._ssh
    
    # Changes SSH config to a new one
    def reload_ssh(self, confname):
        self._ssh = ssh.SSHConfig(confname)

    # Persist current settings
    def persist():
        with open(configloc, 'w') as file:
            parser.write(file)


# Import settings_instance if you wish to read Meta settings
cfg_meta_instance = MetaConfig()