# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import functools
import io
import os
import os.path
import tarfile
import time
import numpy as np
from logging import getLogger
from pathlib import Path
from pytaridx import IndexedTarFile

from .base import IO_Base, check_extn

LOGGER = getLogger(__name__)


# ------------------------------------------------------------------------------
# Tar Interface
# ------------------------------------------------------------------------------
class IO_Tar (IO_Base):

    # --------------------------------------------------------------------------
    # Public Abstract functions
    # --------------------------------------------------------------------------
    @classmethod
    def get_type(cls):
        return 'taridx'

    @classmethod
    def check_environment(cls):
        return True

    @classmethod
    def file_exists(cls, namespace, key):
        assert isinstance(namespace, str) and isinstance(key, str)

        namespace = check_extn(namespace, '.tar')
        if not os.path.isfile(namespace) or not os.path.isfile(namespace+'.pylst'):
            return False

        tf = IndexedTarFile()
        tf.open(namespace, 'r')
        exists = tf.exist(key)
        tf.close()
        return exists

    @classmethod
    def namespace_exists(cls, namespace):
        assert isinstance(namespace, str)

        namespace = check_extn(namespace, '.tar')
        return os.path.isfile(namespace) and os.path.isfile(namespace+'.pylst')

    # --------------------------------------------------------------------------
    # Private Abstract functions
    # --------------------------------------------------------------------------
    @classmethod
    def _list_keys(cls, namespace, keypattern):

        all_keys = cls.load_index(namespace)[:,0]
        return [p for p in all_keys if Path(p).match(keypattern)]

    @classmethod
    def _move_key(cls, namespace, old, new):
        raise Exception('IO_Tar cannot move keys')

    @classmethod
    def _load_files(cls, namespace, filenames):

        namespace = check_extn(namespace, '.tar')

        # open the tar file
        tf = IndexedTarFile()
        tf.open(namespace, 'r')

        # check if all files are found
        for filename in filenames:
            if not tf.exist(filename):
                tf.close()
                LOGGER.debug(f'File ({filename}) does not exist!')
                return None

        # now, read all files
        files = [tf.read(_) for _ in filenames]
        tf.close()
        return files

    @classmethod
    def _save_files(cls, namespace, filenames, data):

        LOGGER.debug(f'Writing {len(filenames)} files to ({namespace})')
        try:
            namespace = check_extn(namespace, '.tar')
            os.makedirs(os.path.dirname(namespace), exist_ok=True)

            tf = IndexedTarFile()
            tf.open(namespace, 'r+')
            for i, fname in enumerate(filenames):
                d = cls._encode(data[i])
                with io.BytesIO() as stream:
                    stream.write(d)
                    tf.write(fname, stream.getvalue())
            tf.close()
            LOGGER.info(f'Wrote {len(filenames)} files to ({namespace})')
            return True
        except Exception as e:
            LOGGER.error(f'Failed to save files: {e}')
            return False

    @classmethod
    def _remove_files(cls, namespace, filenames):
        raise Exception('IO_Tar cannot delete keys')

    # --------------------------------------------------------------------------
    # IO_Tar Specific Functions
    # --------------------------------------------------------------------------
    @classmethod
    def regenerate_index(cls, filename):

        if not os.path.exists(filename):
            return

        LOGGER.info(f'Regenerating index for ({filename})')
        os.remove(f'{filename}.pylst')
        os.remove(f'{filename}.pytree')

        tf = IndexedTarFile()
        tf.open(filename, 'r+')
        tf.close()
        LOGGER.info(f'Regenerated index for ({filename})')

    @classmethod
    def load_index(cls, namespace, idx_start=0, idx_end=-1):

        namespace = check_extn(namespace, '.tar')
        pylst = f'{namespace}.pylst'
        data = []
        LOGGER.debug(f'Loading index from ({namespace})[{idx_start}:{idx_end}]')
        if not os.path.exists(pylst):
            LOGGER.error(f'Missing taridx file {pylst}')
            return
        with open(pylst) as f:
            if idx_end == -1:
                data = np.genfromtxt(f, delimiter=',', comments=None,
                                     skip_header=idx_start,
                                     dtype=str, encoding='utf8')
            else:
                data = np.genfromtxt(f, delimiter=',', comments=None,
                                     skip_header=idx_start,
                                     max_rows=idx_end - idx_start,
                                     dtype=str, encoding='utf8')

        _, uindices = np.unique(data[:, 0], return_index=True)
        data = data[uindices]
        LOGGER.debug(f'Found {data.shape[0]} unique entries')
        return data

# ------------------------------------------------------------------------------
