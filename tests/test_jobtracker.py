# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
#!/usr/bin/env python
# ------------------------------------------------------------------------------

import yaml

from mummi_core.workflow.jobTracker import JobTracker
from mummi_core.utils import Naming

# ------------------------------------------------------------------------------

CREATESIM_JOB_SPEC = """
job_type: createsim

config:
  jobbin:         createsim
  jobname:        createsim
  jobdesc:        CreateSim ({})

  nnodes:         1
  nprocs:         1
  cores per task:         1
  walltime:       '04:00:00'

dir_sim: cg

variables:
  simname: null # value supplied by jobTracker
  timestamp: null # value supplied by jobTracker
  inpath: $MUMMI_ROOT/patches

  outpath: dummy_outpath
  locpath: dummy_locpath

  outfile: 'createsims.out'

script: |
  source $MUMMI_CODE/setup/dbr/load_client.sh

  mkdir -p {outpath}
  mkdir -p {locpath}; cd {locpath}

  # analysis command
  # finalize the two!
  $(LAUNCHER) sh -c createsim \
    --fstype mummi \
    --patch {simname} \
    --inpath {inpath} \
    --outpath {outpath} \
    --logpath {locpath} \
    --loglevel 1 \
    --gromacs gmx \
    --mpi \"autobind-24 gmx mdrun\" \
    --mdrunopt \" -rdd 2.0 -ntomp 4 -dd 4 3 2 -nt 96\" \
    >> {outfile} 2>&1

  wait
"""

def test_jobtracker():

	# get adapter batch and iointerface
	adapter_batch = {'args':None, 'mpi': 'spectrum', 'host': 'summit', 'type': 'flux'}
	iointerface = 'redis'

	simname = 'somepatch_48'
	total_nodes = 5 # calculated

	# test what use to be abstract commands in old jobtracker
	for job_spec in [CREATESIM_JOB_SPEC]:
		job = yaml.load(job_spec, Loader=yaml.Loader)

		jt = JobTracker(
				job,
				total_nodes,
				iointerface,
				adapter_batch)

		print(jt.command(simname))
		print(jt.is_setup(simname))
		print(jt.dir_sim(simname))

# ------------------------------------------------------------------------------

if __name__ == '__main__':

	Naming.init()
	test_jobtracker()