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

from abc import ABC, abstractmethod
import logging
from typing import Dict, List, Any, Union, Type

LOGGER = logging.getLogger(__name__)

# A useful type to represent JSON in Python
JSON = Union[Dict[str, Any], List[Any], int, str, float, bool, Type[None]]

# -----------------------------------------------------------------------------
class BrokerInterface(ABC):
    """
    Represent an instance of a message broker client.
    For example a RabbitMQ client, you can connect to it, send messages
    receive messages and close the connection.
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        """
        Ensure subclass implement all the abstrac method 
        defined in the interface. Errors will be raised 
        if all methods aren't overridden.
        """
        return (hasattr(subclass, "__str__") and
                callable(subclass.__str__) and
                hasattr(subclass, "connect") and
                callable(subclass.connect) and
                hasattr(subclass, "close") and
                callable(subclass.close) and
                hasattr(subclass, "send") and
                callable(subclass.send) and
                hasattr(subclass, "receive") and
                callable(subclass.receive) or
                NotImplemented)

    @abstractmethod
    def __str__(self) -> str:
        """ Return a string representation of the broker """
        raise NotImplementedError

    @abstractmethod
    def connect(self, credentials: any) -> any:
        """ Connect to the broker and return the connection if applicable """
        raise NotImplementedError
    
    @abstractmethod
    def close(self):
        """ Close the connection to the broker """
        raise NotImplementedError
    
    @abstractmethod
    def send(self, data: any):
        """ Send a message to the broker """
        raise NotImplementedError

    @abstractmethod
    def receive(self) -> any:
        """ Receive a message to the broker """
        raise NotImplementedError
# -----------------------------------------------------------------------------