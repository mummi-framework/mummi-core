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

import json
import numpy as np
from typing import TypeVar
import signal

# TODO: Must be replaced by from typing import Self with python 3.11
SelfSignalHandler = TypeVar("SelfSignalHandler", bound="SignalHandler")

# -----------------------------------------------------------------------------
class SignalHandler(object):
    """
    A signal handler that can be used as follow:
        with SignalHandler(signal.SIGINT) as handler:
            # to something
            # if we received SIGINT we can return or break
            if handler.interrupted:
                break
    """
    def __init__(self: SelfSignalHandler, sig = signal.SIGINT) -> None:
        self.sig = sig

    def __enter__(self: SelfSignalHandler) -> SelfSignalHandler:
        self.interrupted = False
        self.released = False
        self.original_handler = signal.getsignal(self.sig)
        
        def handler(signum, frame) -> None:
            self.release()
            self.interrupted = True

        signal.signal(self.sig, handler)
        return self

    def __exit__(self: SelfSignalHandler, type, value, tb) -> None:
        self.release()

    def release(self: SelfSignalHandler) -> bool:
        if self.released:
            return False
        signal.signal(self.sig, self.original_handler)
        self.released = True
        return True

class NumpyEncoder(json.JSONEncoder):
    """
    Special JSON encoder for numpy types. Useful to serialize
    numpy array into JSON with json.dumps()/json.loads()
    """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)
# -----------------------------------------------------------------------------