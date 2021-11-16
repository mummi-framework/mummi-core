# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
from .utils.timer import Timer
from .utils.logger import init_logger
from .utils.naming import MuMMI_NamingUtils as Naming

from .interfaces import get_interfaces, get_io

from logging import getLogger
LOGGER = getLogger(__name__)

# ------------------------------------------------------------------------------
# initialization of MuMMI
# ------------------------------------------------------------------------------
def init(create_root_dir=False):

    import os

    # already initialized!
    if Naming.MUMMI_ROOT != '':
        return Naming.MUMMI_ROOT

    pwd = os.path.dirname(os.path.realpath(__file__))
    print ('\n> Initializing MuMMI ({})'.format(pwd))

    Naming.init()
    if create_root_dir:
        create_root()

    return Naming.MUMMI_ROOT


# ------------------------------------------------------------------------------
def create_root():
    Naming.create_root()


def create_simdir(simname):
    Naming.create_root(simname)


# ------------------------------------------------------------------------------
def get_hostname(contract_hostname = False):

    import socket
    import fnmatch

    hostname = socket.getfqdn()
    KNOWN_HOSTS = {'galaxy*': 'lassen', '*lassen*': 'lassen', '*summit*': 'summit'}

    for k,v in KNOWN_HOSTS.items():
        if fnmatch.fnmatch(hostname, k):
            return v if contract_hostname else hostname

    LOGGER.error('Unidentified hostname: {}'.format(hostname))
    return hostname


def get_resource_counts():

    import multiprocessing

    hostname = get_hostname(contract_hostname=True)
    if hostname == 'lassen':
       ncores, ngpus = 42, 4
    elif hostname == 'summit':
       ncores, ngpus = 42, 6
    else:
        LOGGER.error('Unidentified hostname: {}'.format(hostname))
        return multiprocessing.cpu_count(), 1

    return ncores, ngpus

# ------------------------------------------------------------------------------
