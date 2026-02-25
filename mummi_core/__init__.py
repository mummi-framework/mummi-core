#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights
# reserved. LLNL-CODE-827197. This work was produced at the Lawrence Livermore
# National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44)
# between the U.S. Department of Energy (DOE) and Lawrence Livermore National
# Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers,
# notice of U.S. Government Rights and license terms and conditions.
# -----------------------------------------------------------------------------

from .utils.timer import Timer
from .utils.logger import init_logger
from .utils.utilities import read_specs
from .utils.naming import MuMMI_NamingUtils as Naming

from .interfaces import get_interfaces, get_io
from .brokers import get_broker_interfaces, get_broker

from logging import getLogger
LOGGER = getLogger(__name__)

HAS_BEEN_INIT = False

# ------------------------------------------------------------------------------
# initialization of MuMMI
# ------------------------------------------------------------------------------
def init(create_root_dir=False):

    import os
    global HAS_BEEN_INIT

    # already initialized!
    if Naming.MUMMI_ROOT != '':
        return Naming.MUMMI_ROOT

    pwd = os.path.dirname(os.path.realpath(__file__))
    print ('\n> Initializing MuMMI ({})'.format(pwd))

    Naming.init()
    if create_root_dir:
        create_root()
    
    HAS_BEEN_INIT = True

    return Naming.MUMMI_ROOT

def is_init():
    global HAS_BEEN_INIT
    return HAS_BEEN_INIT

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
    KNOWN_HOSTS = {"galaxy*": "lassen", "*lassen*": "lassen", "*summit*": "summit", "*frontier*": "frontier"}

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
    elif hostname == 'frontier':
       ncores, ngpus = 56, 8
    else:
        LOGGER.error('Unidentified hostname: {}'.format(hostname))
        return multiprocessing.cpu_count(), 1

    return ncores, ngpus

# ------------------------------------------------------------------------------
