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

from .base import BrokerInterface
from .rabbitmq_rpc import RPCServer, RPCClient

KNOWN_BROKER_INTERFACES = ["rabbitmq"]

def get_broker_interfaces():
    return KNOWN_BROKER_INTERFACES

def get_broker(_):
    if _ == "rabbitmq":
        from .rabbitmq import RabbitMQBroker
        interface = RabbitMQBroker
    else:
        raise ValueError(f"Invalid Broker interface requested ({_})")

    # interface.check_environment()
    return interface

# -----------------------------------------------------------------------------