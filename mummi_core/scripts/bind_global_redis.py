# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import sys
from logging import getLogger

import mummi_core

LOGGER = getLogger(__name__)


# ------------------------------------------------------------------------------
def main():
    assert (len(sys.argv) == 3)
    mummi_core.init()
    io_redis = mummi_core.get_io('redis')
    io_redis.bind_global_redis(sys.argv[1], sys.argv[2])


if __name__ == '__main__':
    main()

# ------------------------------------------------------------------------------
