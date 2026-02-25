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

import os
import logging
from mummi_core import Naming

LOGGER = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
def flux_uri(override_uri_from_file = False):
    # we want to read the uri from the server info and not the environment
    # flux_server.info contains the address of master flux node (rank 0)
    # if the workflow is launched on rank 0, this is same as FLUX_URI
    # if not, FLUX_URI is overridden to reflect the rank it is launched on
    # in that case, flux cannot figure out the correct URI
    uri = None
    try:
        # read from environment
        uri_env = os.environ.get("FLUX_URI", None)
        LOGGER.info(f'Read local Flux URI from env: [{uri_env}]')

        # read from file
        fname = os.path.join(Naming.dir_root('flux'), 'flux_server.info')
        if not os.path.isfile(fname):
            LOGGER.error(f'Failed to find Flux URI file ({fname})')
        else:
            with open(fname) as fp:
                uri = fp.read().strip()
            LOGGER.info(f'Read Flux URI from server file: [{uri}]')

        # override if needed
        if override_uri_from_file:
            os.environ["FLUX_URI"] = uri
            print(f'Over-riding FLUX_URI from ({uri_env}) to ({uri})')

    except Exception as e:
        LOGGER.error(f'Failed to fetch Flux URI: {e}')

    return uri

# ------------------------------------------------------------------------------
