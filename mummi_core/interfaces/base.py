# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import io
import logging
import os
import numpy as np
import yaml
import time
import datetime
import shutil
from abc import ABC, abstractmethod
from .default_functions import write_npz, read_npz

LOGGER = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------------------------
def check_extn(filename, extn):
    fe = os.path.splitext(filename)[1]
    return filename if fe == extn else filename + extn


def check_filename(namespace, filename):
    ns = os.path.splitext(namespace)[0]
    if os.path.split(ns)[-1] != os.path.splitext(filename)[0]:
        return os.path.join(ns, filename)
    else:
        return namespace


# ------------------------------------------------------------------------------
# Abstract class for I/O interfaces
# ------------------------------------------------------------------------------
class IO_Base(ABC):

    # --------------------------------------------------------------------------
    # Public Abstract functions
    # --------------------------------------------------------------------------
    @classmethod
    @abstractmethod
    def get_type(cls):
        raise NotImplementedError('Abstract method should be implemented by child class')

    @classmethod
    @abstractmethod
    def check_environment(cls):
        raise NotImplementedError('Abstract method should be implemented by child class')

    @classmethod
    @abstractmethod
    def file_exists(cls, namespace, key):
        raise NotImplementedError('Abstract method should be implemented by child class')

    @classmethod
    @abstractmethod
    def namespace_exists(cls, namespace):
        raise NotImplementedError('Abstract method should be implemented by child class')

    # --------------------------------------------------------------------------
    # Private Abstract functions
    # --------------------------------------------------------------------------
    @classmethod
    @abstractmethod
    def _list_keys(cls, namespace, keypattern):
        raise NotImplementedError('Abstract method should be implemented by child class')

    @classmethod
    @abstractmethod
    def _move_key(cls, namespace, old, new):
        raise NotImplementedError('Abstract method should be implemented by child class')

    @classmethod
    @abstractmethod
    def _load_files(cls, namespace, filenames):
        raise NotImplementedError('Abstract method should be implemented by child class')

    @classmethod
    @abstractmethod
    def _save_files(cls, namespace, filenames, data):
        raise NotImplementedError('Abstract method should be implemented by child class')

    @classmethod
    @abstractmethod
    def _remove_files(cls, namespace, filenames):
        raise NotImplementedError('Abstract method should be implemented by child class')

    # --------------------------------------------------------------------------
    # Public interface
    # --------------------------------------------------------------------------
    @classmethod
    def list_keys(cls, namespace, keypattern):

        keys = cls._list_keys(namespace, keypattern)
        keys = [os.path.basename(k) for k in keys]
        return list(set(keys))

    @classmethod
    def move_key(cls, namespace, key, prefix='done', suffix='.npz'):

        old = check_extn(key, suffix)
        new = prefix + '-' + key
        cls._move_key(namespace, old, new)

    @classmethod
    def load_files(cls, namespace, filenames):

        if isinstance(filenames, list):
            return cls._load_files(namespace, filenames)

        if isinstance(filenames, str):
            return cls._load_files(namespace, [filenames])[0]

        raise ValueError(f'Incorrect arguments (keys={type(keys)}). '
                         f'Need a filename or a list of filenames')

    @classmethod
    def save_files(cls, namespace, keys, data):

        assert isinstance(keys, list) == isinstance(data, list)

        if isinstance(keys, list) and isinstance(data, list):
            assert len(keys) == len(data)
            return cls._save_files(namespace, keys, data)

        if isinstance(keys, str):
            return cls._save_files(namespace, [keys], [data])

        raise ValueError(f'Incorrect arguments (keys={type(keys)}). '
                         f'Need a filename or a list of filenames')

    @classmethod
    def remove_files(cls, namespace, keys):

        if isinstance(keys, list):
            return cls._remove_files(namespace, keys)

        if isinstance(keys, str):
            return cls._remove_files(namespace, [keys])[0]

        raise ValueError(f'Incorrect arguments (keys={type(keys)}). '
                         f'Need a filename or a list of filenames')

    @classmethod
    def load_npz(cls, namespace, keys, reader_func=read_npz):

        if isinstance(keys, list):
            keys = [check_extn(k, '.npz') for k in keys]
            data = cls._load_files(namespace, keys)
            return [reader_func(io.BytesIO(d)) for d in data]

        elif isinstance(keys, str):
            keys = check_extn(keys, '.npz')
            data = cls._load_files(namespace, [keys])[0]
            return reader_func(io.BytesIO(data))

        raise ValueError(f'Incorrect arguments (keys={type(keys)}). '
                         f'Need a filename or a list of filenames')

    @classmethod
    def save_npz(cls, namespace, keys, data, writer_func=write_npz):

        if isinstance(keys, list):
            keys = [check_extn(k, '.npz') for k in keys]
            dbytes = [writer_func(io.BytesIO(), d).getvalue() for d in data]
            return cls._save_files(namespace, keys, dbytes)

        if isinstance(keys, str):
            keys = check_extn(keys, '.npz')
            dbytes = writer_func(io.BytesIO(), data).getvalue()
            return cls._save_files(namespace, [keys], [dbytes])

        raise ValueError(f'Incorrect arguments (keys={type(keys)}). '
                         f'Need a filename or a list of filenames')

    # --------------------------------------------------------------------------
    # Base functionality
    # --------------------------------------------------------------------------
    @classmethod
    def _encode(cls, _):
        # TODO: additional types
        if isinstance(_, bytes):
            return _
        if isinstance(_, str):
            return _.encode('utf-8')
        raise Exception(f'Unhandled data type {type(_)}')

    @classmethod
    def _wmode(cls, _):
        # TODO: additional types
        if isinstance(_, bytes):
            return 'wb'
        if isinstance(_, str):
            return 'w'
        raise Exception(f'Unhandled file mode {type(_)}')

    @classmethod
    def take_backup(cls, filename, suffix=None):
        if os.path.isfile(filename):
            file = filename + '.bak'

            if suffix is not None:
                file = f'{file}.{suffix}'

            shutil.move(filename, file)
            LOGGER.info(f'Saved backup ({file})')

    @classmethod
    def save_checkpoint(cls, filename, data, use_tstamp=False):

        # write new!
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts)

        suffix = st.strftime('%Y%m%d_%H%M%S') if use_tstamp else None
        cls.take_backup(filename, suffix)

        st = st.strftime('%Y-%m-%d %H:%M:%S')
        with open(filename, 'w') as outfile:
            data['ts'] = st
            yaml.dump(data, outfile, default_flow_style=False)

        LOGGER.info(f'Saved checkpoint file ({filename}) at {st}')

    @classmethod
    def load_checkpoint(cls, filename, loader=yaml.FullLoader):

        if not os.path.isfile(filename):
            LOGGER.info(f'Checkpoint file ({filename}) does not exist!')
            return dict()

        try:
            with open(filename, 'r') as infile:
                data = yaml.load(infile, Loader=loader)
        except:
            LOGGER.error(f'Checkpoint file ({filename}) failed to load!')
            return dict()

        if not data:
            LOGGER.error(f'Checkpoint file ({filename}) failed to load!')
            return dict()

        LOGGER.info(f'Restored checkpoint file ({filename}) from {data["ts"]}')
        return data

    @classmethod
    def send_signal(cls, path, key):
        file = os.path.join(path, key)
        with open(file, 'w') as fp:
            fp.write('1')
        LOGGER.info(f'Saved signal ({file})')

    @classmethod
    def test_signal(cls, path, key):
        if key == '':
            return False
        return os.path.isfile(os.path.join(path, key))

# --------------------------------------------------------------------------
