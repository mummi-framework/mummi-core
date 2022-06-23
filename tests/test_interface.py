#!/usr/bin/env python3

# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------

import numpy as np
import shutil, logging, sys, time, pickle, atexit

import mummi_core
from mummi_core.utils import timeout, Naming

LOGGER = logging.getLogger(__name__)

default_io = mummi_core.get_io('taridx')


# ------------------------------------------------------------------------------

def print_separator():
    print('\n-----------------\n')

# ------------------------------------------------------------------------------

def test_keys(iointerface=default_io):
    print('TEST IO: keys')

    iointerface.save_files('_test_io/dir', 'testkey', 'testdata')
    iointerface.save_files('_test_io/dir', ['testkey2', 'testkey3'], ['testdata2', 'testdata3'])
    print(iointerface.load_files('_test_io/dir', 'testkey'))
    print(iointerface.load_files('_test_io/dir', ['testkey2', 'testkey3']))
    print(f'Good namespace exists: {iointerface.namespace_exists("_test_io/dir")}')
    print(f'Good key exists: {iointerface.file_exists("_test_io/dir", "testkey")}')
    print(f'Bad namespace exists: {iointerface.namespace_exists("_test_io/baddir")}')
    print(f'Bad key exists: {iointerface.file_exists("_test_io/dir", "badkey")}')
    print(f'Key in bad namespace exists: {iointerface.file_exists("_test_io/baddir", "testkey")}')


def test_npz(iointerface=default_io):
    print('TEST IO: npz')
    arrays = {'a':np.random.rand(4, 6), 'b':np.random.rand(3, 5)}
    iointerface.save_npz('_test_io/file', 'key', arrays)
    loaded = iointerface.load_npz('_test_io/file', 'key')
    maxVal = 0.0
    for key in arrays:
        difference = np.subtract(arrays[key], loaded[key])
        maxVal = max(maxVal, np.max(difference))
    print("Maximum difference read: {}".format(maxVal))


def test_checkpoint(iointerface=default_io):
    if iointerface.get_type() != 'simple':
        return
    print('TEST IO: checkpoints')
    iointerface.save_checkpoint('_test_io/test_checkpoint', {'a':1, 'b':2})
    data = iointerface.load_checkpoint('_test_io/test_checkpoint')
    print(data)
    iointerface.send_signal('_test_io/', 'test_signal.txt')
    print(iointerface.test_signal('_test_io/', 'test_signal.txt'))
    iointerface.take_backup('_test_io/test_signal.txt')


def test_saveload(iointerface=default_io):
    print('TEST IO: files')
    iointerface.save_files('_test_io/test_namespace', 'test_key', 'blahblahblah')
    print(iointerface.load_files('_test_io/test_namespace', 'test_key'))


def test_performance(iointerface=default_io):
    print('TEST IO: performance')

    N = 500
    keys = [f'k_{i}' for i in range(N)]
    data = [f'd_{i}' for i in range(N)]

    t0 = time.time()
    iointerface.save_files('_test_io/dir', keys, data)
    iointerface.load_files('_test_io/dir', keys)
    print(len(iointerface.list_keys('_test_io/dir', '*')))
    print(f'Time: {time.time() - t0} sec')


def test_heterogenous(iointerface=default_io):
    print('TEST IO: heterogenous')

    iointerface.save_files('_test_io/dir', ['testkey2', 'testkey3', 'testkey4'], 
        [pickle.dumps(5), 'testdata3', pickle.dumps({'a':5})])
    print(iointerface.load_files('_test_io/dir', ['testkey2', 'testkey3', 'testkey4']))


def cleanup():
    shutil.rmtree('_test_io', ignore_errors=True)
    print('Cleaning up tests')

# ------------------------------------------------------------------------------

if __name__ == '__main__':
    mummi_core.init_logger(level=1)

    Naming.init()

    for io in ['simple', 'taridx']:
        iointerface = mummi_core.get_io(io)

        test_keys(iointerface)
        print_separator()
        test_npz(iointerface)
        print_separator()
        test_checkpoint(iointerface)
        print_separator()
        test_saveload(iointerface)
        print_separator()
        test_performance(iointerface)
        print_separator()
        test_heterogenous(iointerface)
        print_separator()

cleanup()
atexit.register(cleanup)

# ------------------------------------------------------------------------------
