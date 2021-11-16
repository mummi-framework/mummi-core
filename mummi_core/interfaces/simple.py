# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import glob
import os
import os.path
import shutil
from logging import getLogger
from .base import IO_Base

LOGGER = getLogger(__name__)


# ------------------------------------------------------------------------------
# Tar Interface
# ------------------------------------------------------------------------------
class IO_Simple (IO_Base):

    # --------------------------------------------------------------------------
    # Public Abstract functions
    # --------------------------------------------------------------------------
    @classmethod
    def get_type(cls):
        return 'simple'

    @classmethod
    def check_environment(cls):
        return True

    @classmethod
    def file_exists(cls, namespace, key):
        assert isinstance(namespace, str) and isinstance(key, str)
        return os.path.isfile(os.path.join(namespace, key))

    @classmethod
    def namespace_exists(cls, namespace):
        assert isinstance(namespace, str)
        return os.path.isdir(namespace)

    # --------------------------------------------------------------------------
    # Private Abstract functions
    # --------------------------------------------------------------------------
    @classmethod
    def _list_keys(cls, namespace, keypattern):
        return glob.glob(os.path.join(namespace, keypattern))

    @classmethod
    def _move_key(cls, namespace, old, new):
        LOGGER.debug(f'moving ({old}) to ({new}) in namespace ({namespace})')
        shutil.move(os.path.join(namespace, old), os.path.join(namespace, new))

    @classmethod
    def _load_files(cls, namespace, filenames):

        # TODO: think about how to load ASCII data
        def _read(f):
            with open(f, 'rb') as fp:
                d = fp.read()
            return d

        filenames = [os.path.join(namespace, _) for _ in filenames]

        # check if all files are found
        for filename in filenames:
            if not os.path.isfile(filename):
                LOGGER.debug(f'File ({filename}) does not exist!')
                return None

        data = [_read(_) for _ in filenames]
        return data

    @classmethod
    def _save_files(cls, namespace, filenames, data):

        LOGGER.debug(f'Writing {len(filenames)} files to ({namespace})')
        try:
            os.makedirs(namespace, exist_ok=True)
            filenames = [os.path.join(namespace, _) for _ in filenames]
            for i,fname in enumerate(filenames):
                mode = cls._wmode(data[i])
                with open(fname, mode) as fp:
                    fp.write(data[i])
            LOGGER.info(f'Wrote {len(filenames)} files to ({namespace})')
            return True
        except Exception as e:
            LOGGER.error(f'Failed to save files: {e}')
            return False

    @classmethod
    def _remove_files(cls, namespace, filenames):

        filenames = [os.path.join(namespace, _) for _ in filenames]
        for filename in filenames:
            if not os.path.isfile(filename):
                LOGGER.debug(f'File ({filename}) does not exist!')
            os.remove(filename)

# ------------------------------------------------------------------------------
