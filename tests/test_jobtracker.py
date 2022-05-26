#!/usr/bin/env python3

# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------

import os
import yaml

from mummi_core.workflow.jobTracker import JobTracker
from mummi_core.utils import Naming


# ------------------------------------------------------------------------------
def test_jobtracker(config_file, simname):
    print(f'> testing ({config_file}')
    iointerface = 'simple'
    total_nodes = 10
    adapter_batch = {'args': None, 'mpi': 'spectrum', 'host': 'summit', 'type': 'flux'}

    with open(config_file, 'r') as data:
        config = yaml.load(data, Loader=yaml.FullLoader)
        jt = JobTracker(config, total_nodes, iointerface, adapter_batch,
                        enable_scheduling=False)

        print(f'---------- begin ({config_file})---------------')
        print(jt.command(simname))
        print(f'---------- end ({config_file})-----------------')


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    Naming.init()
    for f in ['createsim', 'cg']:
        config_file = os.path.join(Naming.MUMMI_SPECS, 'workflow', f'jobs_{f}.yaml')
        test_jobtracker(config_file, 'pfpatch_012345678901')

# ------------------------------------------------------------------------------
