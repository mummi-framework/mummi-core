# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# This is IO interface used in MuMMI
# It provides a single interface with change-able backend
# ------------------------------------------------------------------------------


KNOWN_INTERFACES = ['simple', 'taridx', 'redis']


def get_interfaces():
    return KNOWN_INTERFACES


def get_io(_):
    if _ == 'simple':
        from .simple import IO_Simple
        interface = IO_Simple

    elif _ == 'taridx':
        from .tar import IO_Tar
        interface = IO_Tar

    elif _ == 'redis':
        from .redis import IO_Redis
        interface = IO_Redis

    else:
        raise ValueError(f'Invalid IO interface requested ({_})')

    interface.check_environment()
    return interface

# ------------------------------------------------------------------------------
