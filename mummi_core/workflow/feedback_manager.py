# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
import enum
import socket
from abc import ABC, abstractmethod


# ------------------------------------------------------------------------------
# Two use cases of of feedback:
#   worker : creates the relevant data and puts in files
#   manager: reads data from workers, aggregats, and performs feedback
# ------------------------------------------------------------------------------
class FeedbackManagerType(enum.Enum):
    Unknown = 0
    Worker = 1
    Manager = 2


# ------------------------------------------------------------------------------
# Abstract class for a Selector used by MuMMI
# ------------------------------------------------------------------------------
class FeedbackManager(ABC):

    # --------------------------------------------------------------------------
    def __init__(self, type, name):

        assert isinstance(name, str)
        assert isinstance(type, FeedbackManagerType)
        assert type in [FeedbackManagerType.Worker, FeedbackManagerType.Manager]

        self.type = type
        self.name = name
        self.hostname = socket.getfqdn().split('.')[0]

    def __str__(self):
        return 'FeedbackManager (type = {}; name = {}; host = {})'\
                                   .format(self.type, self.name, self.hostname)

    def __repr__(self):
        return self.__str__()

    # --------------------------------------------------------------------------
    # Abstract API for a MuMMI FeedbackManager
    @abstractmethod
    def load(self):
        raise NotImplementedError('Abstract method should be implemented by child class')

    @abstractmethod
    def aggregate(self):
        raise NotImplementedError('Abstract method should be implemented by child class')

    @abstractmethod
    def report(self):
        raise NotImplementedError('Abstract method should be implemented by child class')

    @abstractmethod
    def checkpoint(self):
        raise NotImplementedError('Abstract method should be implemented by child class')

    @abstractmethod
    def restore(self):
        raise NotImplementedError('Abstract method should be implemented by child class')

    @abstractmethod
    def test(self):
        raise NotImplementedError('Abstract method should be implemented by child class')

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
