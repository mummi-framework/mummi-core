# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
import os
from typing import Tuple, List, ItemsView
import time, datetime, importlib
from functools import partial
from multiprocessing import Pool
import uuid
from mummi_core.utils import Naming

from maestrowf.datastructures.core import StudyStep
from maestrowf.interfaces import ScriptAdapterFactory
from maestrowf.abstracts.enums import CancelCode, SubmissionCode, JobStatusCode, State

import mummi_core
from mummi_core import Naming
from mummi_core.utils.utilities import partition_list, sig_ign_and_rename_proc
from .job import Job, JOB_TYPES, SimulationStatus

from logging import getLogger

LOGGER = getLogger(__name__)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class JobTracker:
    """Abstract base class for a job tracker"""

    def __init__(self, job_desc, total_nodes,
                 iointerface, adapter_batch,
                 enable_scheduling=True):

        self.job_desc = job_desc
        self.type = job_desc['job_type']
        self.config = job_desc['config']

        self.iointerface = iointerface
        adapter_type = adapter_batch['type']
        assert isinstance(self.type, str)
        assert self.type in JOB_TYPES

        assert isinstance(adapter_batch, dict)
        assert isinstance(adapter_type, str)

        self.do_scheduling = enable_scheduling
        try:
            self.adapter = ScriptAdapterFactory.get_adapter(adapter_type)(**adapter_batch)
        except Exception:
            self.adapter = None
            self.do_scheduling = False

        LOGGER.info(f"[{self.type}] Initializing JobTracker (assigned nodes = {total_nodes})")

        # flags to check the status!
        (self.flag_success, self.flag_failure) = Naming.status_flags(self.type)

        LOGGER.debug(f'[{self.type}] status flags: ({self.flag_success})({self.flag_failure})')

        # resource requirements for this type of job (PER SIMULATION)
        self.nnodes = int(self.config['nnodes'])
        self.nprocs = int(self.config['nprocs'])
        self.ncores = int(self.config['cores per task'])
        self.ngpus = int(self.config.get('ngpus', 0))

        # job = bundle of simulations
        self.bundle_size = int(self.config.get('bundle_size', 1))
        self.is_gc = bool(self.config.get('is_gc', False))

        # get the host description
        NCORES_PER_NODE, NGPUS_PER_NODE = mummi_core.get_resource_counts()
        LOGGER.debug(f'[{self.type}] resources available: '
                     f'total_nodes = {total_nodes}, '
                     f'cores_per_node = {NCORES_PER_NODE}, '
                     f'gpus_per_node = {NGPUS_PER_NODE}')

        # assertions on these sizes
        assert (self.nnodes == 1)
        assert (self.nprocs >= 1) and (self.nprocs <= NCORES_PER_NODE)
        assert (self.ncores >= 1) and (self.ncores <= NCORES_PER_NODE)
        assert (self.ngpus >= 0) and (self.ngpus <= NGPUS_PER_NODE)
        assert (self.bundle_size >= 1)

        # compute the max jobs of this type that can go on a node
        cores_per_job = int(self.bundle_size * self.ncores)
        gpus_per_job = int(self.bundle_size * self.ngpus)

        LOGGER.debug(f'[{self.type}] resources needed: '
                     f'cores_per_job = {cores_per_job}, '
                     f'gpus_per_job = {gpus_per_job}')

        # figure out the resources
        max_jobs_pernode = NCORES_PER_NODE // cores_per_job
        if gpus_per_job > 0:
            assert gpus_per_job <= NGPUS_PER_NODE
            max_jobs_pernode = min(max_jobs_pernode, NGPUS_PER_NODE // gpus_per_job)
        assert max_jobs_pernode > 0

        LOGGER.debug(f'[{self.type}] max_jobs_pernode = {max_jobs_pernode}')

        # max number of jobs that can be run
        self.max_jobs_total = int(total_nodes * max_jobs_pernode)
        LOGGER.debug(f'[{self.type}] max_jobs_total = {self.max_jobs_total}')

        # additional flux options
        self.use_broker = bool(self.config.get("use_broker", False))
        self.broker_options = self.config.get("broker_options", {})

        self.workspace = Naming.dir_root('workspace')
        self.hist_file = os.path.join(self.workspace, 'jobtracker.history.csv')

        # state of the job tracker
        self.running = {}  # dict of (job id --> job)
        self.queued = []  # list of simulations to be started
        self.jobCnt = 0  # only for fake run!

        LOGGER.info(
            f'[{self.type}] Initialized JobTracker: '
            f'#nodes = {total_nodes}, '
            f'#max_jobs = {self.max_jobs_total}, '
            f'bundle_size = {self.bundle_size}')

    # --------------------------------------------------------------------------
    def __str__(self):
        return f'JobTracker[{self.type}]: ' \
               f'#max_jobs = {self.max_jobs_total}, ' \
               f'#running = {len(self.running)}, ' \
               f'#queued = {len(self.queued)}'

    def __repr__(self):
        return self.__str__()

    # --------------------------------------------------------------------------
    def njobs_2start(self):
        return 0 if self.max_jobs_total <= len(self.running) else \
            self.nqueued_sims() // self.bundle_size

    def nqueued_sims(self):
        return len(self.queued)

    def nrunning_jobs(self):
        return len(self.running)

    def nrunning_sims(self):
        return int(self.nrunning_jobs() * self.bundle_size)

    def running_sims(self):
        """Get a list of running simulations"""
        running = []
        for job_id, job in self.running.items():
            running.extend(job.sims)
        return running

    def status(self):
        running = {}
        for job_id, job in self.running.items():
            running[job_id] = job.sims

        return {'type': self.type,
                'jobCnt': self.jobCnt,
                'nqueued': len(self.queued),
                'nrunning': len(running),
                'queued': self.queued,
                'running': running}

    def test(self):
        pass

    def write_history(self, event_type, data, comments):

        assert isinstance(data, list)
        if len(data) == 0:
            return

        ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        # write header if the file does not exist
        if not os.path.isfile(self.hist_file):
            with open(self.hist_file, 'a') as fp:
                fp.write('tstamp, job_type, event, id, nrunning, nqueued, comments\n')

        # now, write the data
        with open(self.hist_file, 'a') as fp:
            for d in data:
                fp.write(f'{ts}, {self.type}, {event_type}, {d}, '
                         f'{len(self.running)}, {len(self.queued)}, {comments}\n')

    def dir_sim(self, simname):
        ds = self.job_desc.get('dir_sim') or self.type
        return Naming.dir_sim(ds, simname)

    def is_setup(self, simname):
        return True

    def command(self, simname):
        assert isinstance(simname, (list, str)), "simname must be str or list of strings"
        if isinstance(simname, list):
            assert len(simname) == self.bundle_size, 'simname list must be same size as bundle size'
            assert self.bundle_size == 1, 'currently only bundlesize of 1 supported'
            simname = simname[0]

        variables = {'simname': simname,
                     'timestamp': time.strftime("%Y%m%d-%H%M%S")}

        def process_value(value):
            _type = type(value)

            if _type == list:
                return ''.join(map(process_value, value))

            elif _type == dict:
                # only eval supported for type dict
                assert 'eval' in value, "only eval supported as dict value"
                return eval(value['eval'].format(**variables))

            elif _type == str:
                # substitutes for any variables found
                return value.format(**variables)

            else:
                return value

        # imports 
        imports = self.job_desc.get('imports')
        if imports:
            for name in imports:
                globals()[name] = importlib.import_module(name)

        # add variables
        if self.job_desc.get('variables'):
            for name, val in self.job_desc['variables'].items():
                if val is not None:
                    variables[name] = process_value(val)

        default_command = '\n'.join([
            'ulimit -m 28 10485760',
            'echo \"date:\" `date`',
            'echo \"host:\" `hostname`',
            'echo \"pwd: \" `pwd`',
            'echo \"uri:  \" $FLUX_URI\n',
        ])
        return default_command + process_value(self.job_desc['script'])

    # --------------------------------------------------------------------------
    # MuMMI Workflow functionality
    # --------------------------------------------------------------------------
    @staticmethod
    def check_sim_status(iointerface, job_type, dir_sim, sim_names) -> List[SimulationStatus]:
        """
        Check the status of a simulation using success flags.
        Returns:
            statuses []:       List of statuses Success/Failed/Unknown
        """
        assert isinstance(sim_names, list)
        assert all([isinstance(s, str) for s in sim_names])

        flag_success, flag_failure = Naming.status_flags(job_type)

        statuses = []
        for s in sim_names:
            flag_path = dir_sim(s)

            if iointerface.test_signal(flag_path, flag_success):
                LOGGER.debug(f'[{job_type}] found ({flag_path})/({flag_success})')
                statuses.append(SimulationStatus.Success)

            elif iointerface.test_signal(flag_path, flag_failure):
                LOGGER.debug(f'[{job_type}] found ({flag_path})/({flag_success})')
                statuses.append(SimulationStatus.Failed)

            else:
                statuses.append(SimulationStatus.Unknown)

        return statuses

    def split_sims_on_status(self, sim_names, sim_statuses=None):
        """
        Split a list of sims based on their status.
        Returns:
            sims_success []:       List of sims that finished successfully
            sims_failed []:        List of sims that failed
            sims_unknown []:       List of sims that are neither
        """

        assert isinstance(sim_names, list)
        assert all([isinstance(s, str) for s in sim_names])

        if sim_statuses is not None:
            assert isinstance(sim_statuses, list)
            assert all([isinstance(s, SimulationStatus) for s in sim_statuses])
            assert len(sim_names) == len(sim_statuses)
        else:
            sim_statuses = JobTracker.check_sim_status(self.iointerface, self.type, self.dir_sim, sim_names)

        sims_success = []
        sims_failed = []
        sims_unknown = []

        for i in range(len(sim_names)):
            if sim_statuses[i] == SimulationStatus.Success:
                sims_success.append(sim_names[i])
            elif sim_statuses[i] == SimulationStatus.Failed:
                sims_failed.append(sim_names[i])
            else:
                sims_unknown.append(sim_names[i])

        return sims_success, sims_failed, sims_unknown

    # --------------------------------------------------------------------------
    def add_to_queue(self, sim_names, prepend=False):
        """
        Add some simulations to the queue.
        Returns:
            sim_names []:       the sims that were actually added
        """
        assert isinstance(sim_names, list)
        assert all([isinstance(s, str) for s in sim_names])

        # nothing to do for empty list
        n = len(sim_names)
        if n == 0:
            return sim_names

        LOGGER.info(f'[{self.type}] Adding {n} sims: {self.__str__()}')

        # remove any duplicates
        # TODO: this destroys the order (problem when restoring?)
        sim_names = list(set(sim_names))
        if len(sim_names) < n:
            LOGGER.warning(f'[{self.type}] Found only {len(sim_names)} unique sims')
            n = len(sim_names)

        # don't add those that are already queued
        _is_already_queued = [_ in self.queued for _ in sim_names]
        _rejected, sim_names = partition_list(sim_names, _is_already_queued)
        if len(_rejected) > 0:
            LOGGER.warning(f'[{self.type}] '
                           f'Rejecting {len(_rejected)} already queued sims: {_rejected}')
            self.write_history('rejected', _rejected, 'add_to_queue:already_queued')
            n = len(sim_names)

        # don't add those that are already running
        rsims = self.running_sims()
        _is_already_running = [_ in rsims for _ in sim_names]
        _rejected, sim_names = partition_list(sim_names, _is_already_running)
        if len(_rejected) > 0:
            LOGGER.warning(f'[{self.type}] '
                           f'Rejecting {len(_rejected)} already running sims: {_rejected}')
            self.write_history('rejected', _rejected, 'add_to_queue:already_running')
            n = len(sim_names)

        # finally, add these simulations
        if prepend:
            _tag = 'prepended'
            self.queued = sim_names + self.queued
        else:
            _tag = 'appended'
            self.queued = self.queued + sim_names

        self.write_history(f'{_tag}_to_queue', sim_names, 'add_to_queue')
        LOGGER.debug(f'[{self.type}] {_tag} {n} sims: {self.__str__()}: {sim_names}')
        return sim_names

    # --------------------------------------------------------------------------
    def start_jobs(self, n_jobs):
        """Start queued simulations in bundles of size self.bundle_size.
        Returns:
            n_jobs:         number of jobs started
            sims_started:   names of the sims started
        """
        assert 1 == self.bundle_size
        assert isinstance(n_jobs, int)
        assert n_jobs >= 0
        assert self.nrunning_jobs() <= self.max_jobs_total
        assert all([isinstance(q, str) for q in self.queued])

        if n_jobs == 0:
            return 0, []

        LOGGER.info(self.__str__())

        # ----------------------------------------------------------------------
        # number of jobs to start is the minimum of
        #       the number of jobs asked by the caller
        #       the number of jobs possible for the given resources
        #       the number of jobs possible for the given queue and bundle size
        mn_jobs = min(n_jobs,
                      self.max_jobs_total - self.nrunning_jobs(),
                      self.nqueued_sims() // self.bundle_size)

        if mn_jobs == 0:
            LOGGER.debug(f'[{self.type}] Nothing to do! (njobs = {self.max_jobs_total}),'
                         f'(max_jobs_total - nrunning_jobs = {self.max_jobs_total}-{self.nrunning_jobs()} = {self.max_jobs_total - self.nrunning_jobs()}), '
                         f'(nqueued//bundle = {self.nqueued_sims()}//{self.bundle_size} = {self.nqueued_sims() // self.bundle_size})')
            return 0, []

        n_jobs = mn_jobs
        LOGGER.debug(f'[{self.type}] n_jobs = {n_jobs}')
        assert n_jobs > 0

        # ----------------------------------------------------------------------
        # pick chunks of simulations
        n_sims = n_jobs * self.bundle_size
        sims_started: List[str] = sorted(self.queued[:n_sims])
        self.queued = self.queued[n_sims:]

        LOGGER.debug(f'[{self.type}] sims_to_start = {sims_started}')

        # ----------------------------------------------------------------------
        # Mar 02, 2021. HB commented this piece
        # and replaced with a parallel version
        if self.do_scheduling:
            # May 20: switch back to serial
            if False:
                '''
                for i in range(n_jobs):
                    sim_names = sims_started[i * self.bundle_size:(i + 1) * self.bundle_size]
                    jobid = self.submit_job(sim_names)
                    self.running[jobid] = Job(self.type, jobid, sim_names)
                     LOGGER.debug(f'[{self.type}] Started job {} for {}'.format(jobid, sim_names))
                '''
                for simname in sims_started:
                    _simname, cmd_script, step = self.write_script(simname)
                    LOGGER.debug(f'[{self.type}] submitting script {simname} {cmd_script}')
                    # submit cmd_script to adapter and append (jobid, simname) to queue
                    submit_record = self.adapter.submit(step, cmd_script, self.workspace)
                    if submit_record.submission_code != SubmissionCode.OK:
                        LOGGER.error(f'[{self.type}] Failed to submit a {self.type} job for simname = {simname}')
                        # raise Exception('Failed to submit a {} job'.format(self.type))

                    job_id = submit_record.job_identifier
                    self.running[job_id] = Job(self.type, job_id, [simname])
                    LOGGER.debug(f'[{self.type}] Started job {job_id} for {simname}')

            else:
                # submit sims to write_script, add terminator when write_pool finishes so that submit process knows it's done
                LOGGER.info(
                    f'[{self.type}] START_JOB -- Starting Pooled Script Generation [njobs = {len(sims_started)}]')
                write_pool = Pool(
                    processes=10,
                    initializer=sig_ign_and_rename_proc,
                    initargs=("pool_write_job_tracker",)
                )
                for simname, cmd_script, step in write_pool.imap_unordered(self.write_script, sims_started):
                    LOGGER.debug(f'[{self.type}] submitting script {simname} {cmd_script}')
                    # submit cmd_script to adapter and append (jobid, simname) to queue
                    submit_record = self.adapter.submit(step, cmd_script, self.workspace)
                    if submit_record.submission_code != SubmissionCode.OK:
                        raise Exception(f'Failed to submit a {self.type} job')

                    job_id = submit_record.job_identifier
                    self.running[job_id] = Job(self.type, job_id, [simname])
                    LOGGER.debug(f'[{self.type}] Started job {job_id} for {simname}')
                write_pool.close()

                LOGGER.info(f'[{self.type}] START_JOB -- Ended Pooled Script Generation')

            # ----------------------------------------------------------------------
            LOGGER.info(f'[{self.type}] Started {n_jobs} jobs: {self.__str__()}')
            self.write_history('started', sims_started, 'start_jobs')

        else:
            LOGGER.info(f"[{self.type}] Scheduling disabled")
            for simname in sims_started:
                job_id = str(uuid.uuid4().hex)
                self.running[job_id] = Job(self.type, job_id, [simname])
                self.jobCnt += 1  # Probably not needed.

        assert self.nrunning_jobs() <= self.max_jobs_total
        return n_jobs, sims_started

    # --------------------------------------------------------------------------
    def write_script(self, sims_chunk: str):
        """Create a Maestro study step and cmd_script"""
        assert self.do_scheduling == True

        LOGGER.debug(f"[{self.type}] Creating step for {sims_chunk}...")
        step = self.create_step([sims_chunk])
        LOGGER.debug(f"[{self.type}] Step created: {step}")
        to_be_scheduled, cmd_script, restart_script = \
            self.adapter.write_script(self.workspace, step)

        return sims_chunk, cmd_script, step

    # --------------------------------------------------------------------------
    def update(self):
        """Check all running jobs to update the status of the tracker.
        Returns:
            sims_success = []:      simulations that have finished successfully
            sims_failed = []:       simulations that have failed
        """

        if self.nrunning_jobs() == 0:
            LOGGER.debug(f'[{self.type}] Returning because have no running jobs: {self.__str__()}')
            return [], []

        LOGGER.info(self.__str__())

        sims_success = []  # simulations that have finished successfully
        sims_failed = []  # simulations that have failed
        sims_continue = []  # simulations that need to be continued
        jobs_2_continue = []  # jobs that are still running
        jobs_2_reclaim = []  # jobs that either finished or failed
        jobs_2_cancel = []

        # Initialize termination status set.
        term_set = set([SimulationStatus.Failed])

        # get job statuses
        jobid_jobs: ItemsView[str, Job] = self.running.items()

        LOGGER.debug(f'[{self.type}] Fetching status for {len(self.running)} jobs')
        job_statuses: List[Tuple[bool, bool]] = self.get_jobs_statuses(list(self.running.keys()))

        # get sim statuses
        '''
        LOGGER.debug(f'[{self.type}] Fetching status for all sims in {len(self.running)} jobs')
        sim_status_pool = Pool(processes=10)
        sim_statuses: List[List[SimulationStatus]] = sim_status_pool.map(
            partial(JobTracker.check_sim_status, self.iointerface, self.type, self.dir_sim), 
            [job.sims for (jobid, job) in jobid_jobs]
           )  
        '''
        # switched back to serial use 
        if False:
            with Pool(processes=10) as sim_status_pool:
                sim_statuses: List[List[SimulationStatus]] = sim_status_pool.map(
                    partial(JobTracker.check_sim_status, self.iointerface, self.type, self.dir_sim),
                    [job.sims for (jobid, job) in jobid_jobs]
                )
        else:
            sim_statuses = [JobTracker.check_sim_status(self.iointerface, self.type, self.dir_sim, job.sims) for
                            (jobid, job) in jobid_jobs]

        # look at each running job
        for i, (jobid, job) in enumerate(jobid_jobs):

            nsims = len(job.sims)
            assert nsims == self.bundle_size

            # ------------------------------------------------------------------
            # job status = True:    let it run
            #              False:   kill (if needed) and reclaim resources
            job_is_running, job_is_tout = job_statuses[i]

            if not job_is_running and job_is_tout:
                # TODO: this assumes chunk_size = 1
                sim_status = [SimulationStatus.Failed]

            else:
                # check the status of all sims in the bundle
                #       will continue only if status == unknown (not success/failed)
                sim_status = sim_statuses[i]

            # ------------------------------------------------------------------
            # if the job is still running and at least one sim needs to continue
            sims_continue_any = any([s == SimulationStatus.Unknown for s in sim_status])
            sims_term_cancel = all([s in term_set for s in sim_status])
            if job_is_running and sims_continue_any:
                jobs_2_continue.append(jobid)
                continue

            # If the job is running but all underlying simulations have failed
            # reap the job and reclaim the resources.
            jobs_2_reclaim.append(jobid)
            if job_is_running and sims_term_cancel:
                # otherwise, need to reclaim the resources from this job
                jobs_2_cancel.append(jobid)

            LOGGER.debug(
                '[JOBID %s, JOB %s] : Status: %s\t| Running? %s\t| Timedout? %s\t| Continue? %s\t| Cancel? %s\t|',
                jobid, job, str(sim_status), str(job_is_running), str(job_is_tout), str(sims_continue_any),
                str(sims_term_cancel))
            # split the simulations of this job based on status
            _ss, _sf, _sc = self.split_sims_on_status(job.sims, sim_status)
            LOGGER.debug(f'[{self.type}] sims: success = {_ss}')
            LOGGER.debug(f'[{self.type}] sims: failed = {_sf}')
            LOGGER.debug(f'[{self.type}] sims: continue = {_sc}')

            sims_success.extend(_ss)
            sims_failed.extend(_sf)
            sims_continue.extend(_sc)

        # ----------------------------------------------------------------------
        njobs_continue = len(jobs_2_continue)
        njobs_reclaim = len(jobs_2_reclaim)
        njobs_cancel = len(jobs_2_cancel)
        nsims_success = len(sims_success)
        nsims_failed = len(sims_failed)
        nsims_continue = len(sims_continue)

        LOGGER.info(f'[{self.type}] processed all jobs. '
                    f'(#jobs: continue = {njobs_continue}, reclaim = {njobs_reclaim}, cancel = {njobs_continue}), '
                    f'(#sims: success = {nsims_success}, failed = {nsims_failed}, continue = {nsims_continue})')

        self.write_history('found_success', sims_success, 'update')
        self.write_history('found_failed', sims_failed, 'update')

        assert njobs_continue + njobs_reclaim == len(self.running)
        assert nsims_success + nsims_failed + nsims_continue == self.bundle_size * njobs_reclaim

        # when working with a bundle size of 1
        # should not have to cancel a job and find a sim to continue
        if self.bundle_size == 1 and nsims_continue > 0:
            LOGGER.error(
                f'[{self.type}] Found {nsims_continue} sims to continue for bundle_size = {self.bundle_size}: Looks like these sims ended without a flag: {sims_continue}')

        # ----------------------------------------------------------------------
        # kill the jobs and remove from the list
        if jobs_2_cancel:
            LOGGER.debug(f"[{self.type}] Cancelling simulations (njobs_cancel) = {njobs_cancel}")
            self.cancel_jobs(jobs_2_cancel)

        if njobs_reclaim > 0:
            LOGGER.debug(f'[{self.type}] >> TEST: reclaiming jobs {jobs_2_reclaim} from running {self.running.keys()}')
            for j in jobs_2_reclaim:
                self.running.pop(j)
            LOGGER.debug(f'[{self.type}] >> TEST: after reclaiming running {self.running.keys()}')

        # requeue the sims that need to be continued (their job has ended)
        if nsims_continue > 0:
            self.add_to_queue(sims_continue, prepend=True)

        # ----------------------------------------------------------------------
        LOGGER.info(self.__str__())

        # return the successful and failed sims for further pipeline
        return sims_success, sims_failed

    # --------------------------------------------------------------------------
    def restore(self, state, check_for_running_jobs):
        """Check status of all running jobs, and add to queue if needed.
        Returns:
            sims_success = []:      simulations that have finished successfully
            sims_failed = []:       simulations that have failed
        """
        assert self.type == state['type']
        self.jobCnt = state['jobCnt']  # fake: only needed for no_scheduling mode

        jobs_running = state['running']
        sims_queued = list(state['queued'])
        nrunning = len(jobs_running)
        nqueued = len(sims_queued)

        LOGGER.info(
            f'[{self.type}] Restoring JobTracker: running = {len(jobs_running)} jobs, queued = {len(sims_queued)} sims')

        if (nrunning == 0) and (nqueued == 0):
            return [], []

        _data = [f'running={nrunning}', f'queued={nqueued}']
        self.write_history('restore', _data, 'restore')

        # ----------------------------------------------------------------------
        # need to check for running jobs
        if check_for_running_jobs:

            jobs_restored = []
            sims_restored = []
            for jobId, sims in jobs_running.items():
                LOGGER.debug(f'[{self.type}] is job {jobId} running? {sims}')
                if self.is_job_running(jobId)[0]:
                    LOGGER.debug(f'[{self.type}] Restoring job {jobId}: sims = {sims}')
                    self.running[jobId] = Job(self.type, jobId, sims)
                    jobs_restored.append(jobId)
                    sims_restored.extend(sims)

                if self.nrunning_jobs() >= self.max_jobs_total:
                    break

            LOGGER.info(f'[{self.type}] Restored {self.nrunning_jobs()} jobs')
            self.write_history('restored', sims_restored, 'restore')

            # remove the restored jobs from the list
            for j in jobs_restored:
                jobs_running.pop(j)

        # ----------------------------------------------------------------------
        # now, collect the sims of the jobs that were not restored!
        sims_not_restored = []
        for jobId, sims in jobs_running.items():
            sims_not_restored.extend(sims)

        # ----------------------------------------------------------------------
        # TODO: this fix should not be needed
        # filter the ones that are not correctly setup!
        if True:
            _correct = [self.is_setup(s) for s in sims_not_restored]
            sims_not_restored, _rejected = partition_list(sims_not_restored, _correct)

            if len(_rejected) > 0:
                LOGGER.error(
                    f'[{self.type}] Found some running sims that were not setup correctly. Ignoring those: {_rejected}!')
                self.write_history("rejected", _rejected, 'restore:incorrect_setup/running')
                assert False

            _correct = [self.is_setup(s) for s in sims_queued]
            sims_queued, _rejected = partition_list(sims_queued, _correct)

            if len(_rejected) > 0:
                LOGGER.error(
                    f'[{self.type}] Found some queued sims that were not setup correctly. Ignoring those: {_rejected}!')
                self.write_history("rejected", _rejected, 'restore:incorrect_setup/queued')
                assert False

        if False:
            sims_backup = sims_not_restored
            sims_not_restored = [s for s in sims_not_restored if self.is_setup(s)]

            if len(sims_not_restored) != len(sims_backup):
                LOGGER.error(f'[{self.type}] Found some running sims that were not setup correctly. Ignoring those!')
                LOGGER.error(f'[{self.type}] got from checkpoint file: {sims_backup}')
                LOGGER.error(f'[{self.type}] using only: {sims_not_restored}')

            # filter the ones that are not correctly setup!
            sims_backup_q = sims_queued
            sims_queued = [s for s in sims_queued if self.is_setup(s)]

            if len(sims_queued) != len(sims_backup_q):
                LOGGER.error(f'[{self.type}] Found some queued sims that were not setup correctly. Ignoring those!')
                LOGGER.error(f'[{self.type}] got from checkpoint file: {sims_backup_q}')
                LOGGER.error(f'[{self.type}] using only: {sims_queued}')
        # TODO: above fix should not be needed
        # ----------------------------------------------------------------------

        # check the status of these sims
        sims_success, sims_failed, sims_continue = self.split_sims_on_status(sims_not_restored)

        self.write_history('found_success', sims_success, 'restore')
        self.write_history('found_failed', sims_failed, 'restore')

        # now, add them to the queue
        LOGGER.info(f'[{self.type}] Queuing {len(sims_continue)} previously-running sims')
        _radded = self.add_to_queue(sims_continue, prepend=True)

        # now add the queued jobs
        _qadded = self.add_to_queue(sims_queued, prepend=False)

        # ----------------------------------------------------------------------
        LOGGER.info(f'[{self.type}] Restored {nqueued} queued and {nrunning} running jobs')
        LOGGER.info(self.__str__())

        # return the ones that we did not restore so wf can handle them
        return sims_success, sims_failed

    # --------------------------------------------------------------------------
    # Maestro related functionality
    # --------------------------------------------------------------------------
    def create_step(self, sims_bundle):
        """
        Create a StudyStep for CreateSim jobs using config and sim candidates.
        """
        if not self.do_scheduling:
            return None

        assert isinstance(sims_bundle, list)
        assert len(sims_bundle) == self.bundle_size

        if self.bundle_size == 1:
            cname = sims_bundle[0]
        else:
            cname = '_'.join(sims_bundle)

        LOGGER.debug(f'[{self.type}] sims_bundle = {sims_bundle}')

        step = StudyStep()
        step.name = self.config['jobname'] + '-' + cname
        step.description = self.config['jobdesc'].format(cname)

        step.run['cmd'] = self.command(sims_bundle)
        walltime = self.config.get('walltime', None)
        if walltime:
            step.run['walltime'] = walltime

        step.run['nodes'] = self.nnodes
        step.run['procs'] = self.nprocs
        step.run['cores per task'] = self.ncores

        if self.ngpus > 0:
            step.run['gpus'] = self.ngpus

        if self.use_broker:
            step.run['use_broker'] = self.use_broker

        wrapper = self.config.get('wrapper', None)
        if wrapper:
            step.run['wrapper'] = wrapper

        addtl_args = self.config.get('addtl_args', {})
        return step

    # --------------------------------------------------------------------------
    def cancel_jobs(self, jobIds):
        """
        Use Maestro to kill the jobs
            * some of the jobs may already be dead.
            * so make sure it doesnt throw an error
        """
        if not self.do_scheduling:
            return True

        assert isinstance(jobIds, list)

        # cancel our jobs
        retcode = self.adapter.cancel_jobs(jobIds)
        if retcode == CancelCode.OK:
            LOGGER.info(f'[{self.type}] Successfully canceled {len(jobIds)} jobs')
            return True

        if retcode == CancelCode.ERROR:
            LOGGER.error(f'[{self.type}] Failed to cancel jobs')
            return False

        LOGGER.error(f'[{self.type}] Unknown error: {retcode}')
        return False

    def get_jobs_statuses(self, jobIds: List[str]) -> List[Tuple[bool, bool]]:
        """
        Check status of this job via Maestro
        * False: if job is finished or failed (i.e., workflow can reclaim resources)
        * True:  otherwise (for unknown status, we do not want to reclaim)
        """

        # -- job status code
        # OK                    # could query the job properly
        # NOJOBS                # queried, but job not found
        # ERROR                 # could not query the scheduler

        invalid_codes = {JobStatusCode.NOJOBS, JobStatusCode.ERROR}

        # -- valid states
        # State.INITIALIZED,    # maestro initialized, waiting to submit
        # State.PENDING,        # pending start (in the scheduler)
        # State.WAITING,        # waiting for resources (in the scheduler)
        # State.RUNNING,
        # State.FINISHING,
        # State.QUEUED,         # queued (in the scheduler)

        # -- invalid states
        # State.FINISHED,
        # State.FAILED,
        # State.INCOMPLETE,     # not currently used in maestro
        # State.HWFAILURE,
        # State.TIMEDOUT,
        # State.UNKNOWN,
        # State.CANCELLED

        invalid_states = {State.FINISHED, State.FAILED, State.INCOMPLETE,
                          State.HWFAILURE, State.TIMEDOUT, State.CANCELLED,
                          State.UNKNOWN, State.NOTFOUND}

        # now check the status via maestro
        retcode, job_status = self.adapter.check_jobs(jobIds)

        if retcode in invalid_codes:
            LOGGER.debug(f'[{self.type}] Returning due to invalid code. [Code={retcode} : jobid={jobIds}]')
            return []

        return [(job_status[jobId] not in invalid_states, job_status[jobId] == State.TIMEDOUT) for jobId in jobIds]

    # --------------------------------------------------------------------------
    def is_job_running(self, jobId):
        """
        Check status of this job via Maestro
        * False: if job is finished or failed (i.e., workflow can reclaim resources)
        * True:  otherwise (for unknown status, we do not want to reclaim)
        """
        if not self.do_scheduling:
            LOGGER.debug(f'[{self.type}] Returning, adapter is None.')
            return False, False

        # -- job status code
        # OK                    # could query the job properly
        # NOJOBS                # queried, but job not found
        # ERROR                 # could not query the scheduler

        invalid_codes = {JobStatusCode.NOJOBS, JobStatusCode.ERROR}

        # -- valid states
        # State.INITIALIZED,    # maestro initialized, waiting to submit
        # State.PENDING,        # pending start (in the scheduler)
        # State.WAITING,        # waiting for resources (in the scheduler)
        # State.RUNNING,
        # State.FINISHING,
        # State.QUEUED,         # queued (in the scheduler)

        # -- invalid states
        # State.FINISHED,
        # State.FAILED,
        # State.INCOMPLETE,     # not currently used in maestro
        # State.HWFAILURE,
        # State.TIMEDOUT,
        # State.UNKNOWN,
        # State.CANCELLED

        invalid_states = {State.FINISHED, State.FAILED, State.INCOMPLETE,
                          State.HWFAILURE, State.TIMEDOUT, State.CANCELLED,
                          State.UNKNOWN, State.NOTFOUND}

        # now check the status via maestro
        retcode, job_status = self.adapter.check_jobs([jobId])
        LOGGER.debug(f'[{self.type}] Received job status from Maestro: retcode = {retcode}, state = {job_status}')

        if retcode in invalid_codes:
            LOGGER.debug(f'[{self.type}] Returning due to invalid code. [Code={retcode} : jobid={jobId}]')
            return False, False

        return job_status[jobId] not in invalid_states, job_status[jobId] == State.TIMEDOUT

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
