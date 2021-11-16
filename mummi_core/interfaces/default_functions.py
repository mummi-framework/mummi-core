# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import io
import numpy as np


# ------------------------------------------------------------------------------
def write_npz(file, data):
    # assert isinstance(file, str) or isinstance(file, io.BytesIO)
    # assert isinstance(data, dict)
    np.savez_compressed(file, **data)
    return file


def read_npz(file):
    # assert isinstance(file, str) or isinstance(file, io.BytesIO)
    npz_obj = np.load(file, allow_pickle=True)
    data = {key: npz_obj[key] for key in npz_obj.files}
    npz_obj.close()
    return data


def write_string(file, data):
    # assert isinstance(file, str)
    # assert isinstance(data, str)
    with open(file, 'w') as f:
        f.write(data)
    return file


def read_string(file):
    # assert isinstance(file, str)
    with open(file, 'r') as f:
        data = f.read()
    return data


def write_binary(file, data):
    # assert isinstance(file, str)
    # assert isinstance(data, bytes)
    with open(file, 'wb') as f:
        f.write(data)
    return file


def read_binary(file):
    # assert isinstance(file, str)
    with open(file, 'rb') as f:
        data = f.read()
    return data


def write_bytes(bytesIO, data):
    # assert isinstance(bytesIO, io.BytesIO)
    bytesIO.write(data.encode('utf-8'))
    return bytesIO


def read_bytes(bytesIO):
    # assert isinstance(bytesIO, io.BytesIO)
    return bytesIO.getvalue().decode('utf-8')

# ------------------------------------------------------------------------------
