# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import os
import time
from typing import List, Callable
import multiprocessing as mp

import mummi_core
from logging import getLogger
LOGGER = getLogger(__name__)


# ------------------------------------------------------------------------------
def move_to(keys_path: str, keys: List[str], out_path: str):

    if not isinstance(keys, list):
        keys = [keys]

    process_name: str = mp.current_process().name
    LOGGER.debug(f'{process_name}: is moving {len(keys)} keys from ({keys_path}) to ({out_path})')
    os.makedirs(out_path, exist_ok=True)

    for key in keys:
        try:
            os.rename(os.path.join(keys_path, key),  os.path.join(out_path, key))
        except Exception as _expt:
            LOGGER.error(f'Failed to move ({key}): {_expt}')

    LOGGER.debug(f'{process_name}: finished moving {len(keys)} keys from ({keys_path}) to ({out_path})')


# ------------------------------------------------------------------------------
def tar_and_remove(tar_path: str, tar_name: str, keys_path: str,
                   keys: List[str], data: List, writer_func: Callable):

    assert isinstance(tar_path, str) and isinstance(tar_name, str)
    assert isinstance(keys_path, str)
    assert isinstance(keys, list) and isinstance(data, List)
    assert len(data) == len(keys)
    assert callable(writer_func)

    process_name: str = mp.current_process().name
    filename: str = os.path.join(tar_path, f'{tar_name}.{process_name}.tar')

    # tar the data
    LOGGER.debug(f'{process_name}: is tarring {len(keys)} keys into ({filename})')
    try:
        mummi_core.get_io('taridx').save_npz(filename, keys, data, writer_func)
    except Exception as _expt:
        LOGGER.error(f'Failed to tar ({filename}): {_expt}')
        return

    # remove npz files
    LOGGER.debug(f'{process_name}: is removing {len(keys)} keys from ({keys_path})')
    for key in keys:
        npz_path = os.path.join(keys_path, key)
        try:
            os.remove(npz_path)
        except Exception as _expt:
            LOGGER.error(f'Failed to remove ({npz_path}): {_expt}')

    LOGGER.debug(f'{process_name}: removed {len(keys)} keys from ({keys_path})')


# ------------------------------------------------------------------------------
def convert_interface_type(from_interface, to_interface, namespace, keypattern,
                           max_keys=1000000):
    LOGGER.debug(f'Copying {namespace}, {keypattern}')
    init_time = time.time()
    from_io = mummi_core.get_io(from_interface)
    to_io = mummi_core.get_io(to_interface)
    keys = from_io.list_keys(namespace, keypattern)
    keys = keys[:max_keys]
    for key in keys:
        data = from_io.load_npz(namespace, key)
        to_io.save_npz(namespace, key, data)
    LOGGER.debug(f'Copied {len(keys)} keys in {namespace}/{keypattern} ' +
                 f'from ({from_interface}) to ({to_interface}), ' +
                 f'took {time.time() - init_time:.2f} seconds')

# ------------------------------------------------------------------------------
