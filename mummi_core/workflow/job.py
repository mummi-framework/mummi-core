# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import enum


# ------------------------------------------------------------------------------
class SimulationStatus(enum.Enum):
    Unknown = 0  # neither of the below (do continue or restart)
    Success = 1  # simulation has ended
    Failed = 2  # simulation has failed (don't restart)


# TODO: should come from a config

# types of jobs
JOB_TYPES = ['createsim', 'cg', 'backmapping', 'aa']

# chain the types of jobs
JOB_NEXT_QUEUE = {'createsim': 'cg', 'backmapping': 'aa'}


# ------------------------------------------------------------------------------
class Job(object):
    def __init__(self, jtype, id, sims):
        assert isinstance(jtype, str)
        assert isinstance(id, int)
        assert isinstance(sims, list)
        assert jtype in JOB_TYPES
        assert id > 0

        self.type = jtype  # job type
        self.id = id  # job id
        self.sims = sims  # sims running in this job

    def __str__(self):
        return 'Job[{}]: id = {}, sims = {}'.format(self.type, self.id, self.sims)

    def __repr__(self):
        return self.__str__()

# ------------------------------------------------------------------------------
