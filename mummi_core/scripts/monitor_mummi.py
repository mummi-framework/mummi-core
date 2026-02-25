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

# this script is used to create and append to a csv file the curent counts of
# flux jobs. Creates a header of the fields formed from the product of job names and status codes
# in the parameters just below.
# every interval a 'flux jobs' is called and a new CSV row of the job-status counts is added 

# in each interval the endtime amount of remaining time is also checked to allow for cleanup to be performed


from typing import List, Tuple, Dict
from itertools import product
import subprocess
from subprocess import check_output, run
import time
from time import sleep, strftime
import os
from os import makedirs, getenv as env
from os.path import exists
from math import inf
import logging
import sys, argparse, yaml
import traceback

# ------------------------------------------------------------------------------
FLUX_STATUS_CODES = ['R', 'PD', 'CA', 'CD', 'F', 'TO', 'S', 'C']
NNODES = env('MUMMI_NNODES', '')
MUMMI_ROOT = env("MUMMI_ROOT")
MONITOR_DIRPATH    = f'{MUMMI_ROOT}/monitoring'
STATUS_FILEPATH    = f'{MONITOR_DIRPATH}/status_{strftime("%Y%m%d-%H%M%S")}.csv'
RAW_FLUX_LOGS_DIR  = f'{MONITOR_DIRPATH}/raw_flux_logs'
JSON_FLUX_LOGS_DIR = f'{MONITOR_DIRPATH}/json_flux_logs'
RES_FLUX_LOGS_DIR  = f'{MONITOR_DIRPATH}/resource_flux_logs'

flux_format_045="{id.f58:>12} {username:<8.8} {name:<15.15} {status_abbrev:>2.2} {urgency:>4} {priority:>4} {ntasks:>6} {nnodes:>6h} {runtime!F:>8} {nodelist}"
# flux_format_045="{id.f58} {username} {name} {status_abbrev} {urgency} {priority} {ntasks} {nnodes} {runtime} {nodelist}"

# ------------------------------------------------------------------------------
def main(config):

    # --------------------------------------------------------------------------
    # set up logger
    LOG_FMT = '%(asctime)s - %(name)s:%(funcName)s:%(lineno)s - %(levelname)s - %(message)s'
    LOGGER = logging.getLogger()
    LOGGER.setLevel(config['LOGLEVEL'])

    sh = logging.StreamHandler()
    sh.setLevel(config['LOGLEVEL'])
    sh.setFormatter(logging.Formatter(LOG_FMT))
    LOGGER.addHandler(sh)

    try:
        import mummi_core
        LOGGER.info('Successfully imported mummi_core.')

    except ModuleNotFoundError as e:
        LOGGER.error('Failed to import mummi_core.')
        raise e

    # --------------------------------------------------------------------------
    LOGGER.info('Starting to monitor...')
    if not exists(RAW_FLUX_LOGS_DIR):
        makedirs(RAW_FLUX_LOGS_DIR)

    if not exists(JSON_FLUX_LOGS_DIR):
        makedirs(JSON_FLUX_LOGS_DIR)

    if not exists(RES_FLUX_LOGS_DIR):
        makedirs(RES_FLUX_LOGS_DIR)

    # write CSV header
    header_columns = \
        ['JOB_ID', 'NNODES', 'timestamp'] + \
        [f'{name}' for name in config['JOB_NAMES_2_TRACK']] + \
        [f'#{stat}_{name}' for name, stat in product(config['JOB_NAMES_2_COUNT'], FLUX_STATUS_CODES)]
    open(STATUS_FILEPATH, 'w').write(','.join(header_columns) + '\n')

    # calculate endtime of job
    if "SLURM_JOBID" in os.environ:
        JOB_ID = env("SLURM_JOBID", '')
        endtime = get_endtime_slurm(JOB_ID, LOGGER)
    elif "LSB_JOBID" in os.environ:
        JOB_ID = env("LSB_JOBID", '')
        endtime = get_endtime_lsf(JOB_ID, LOGGER)
    else:
        LOGGER.error('Could not find the proper Job ID. SLURM_JOBID and LSB_JOBID not set.')
        endtime == inf

    if endtime == inf:
        LOGGER.warning('MONITOR > job endtime failed to calculate. jobs will not be killed!')

    wf_signal_sent = False

    def kill_all_jobs(job_names, signal: str = "SIGTERM"):
        LOGGER.info(f'MONITOR > time remaining(sec): {int(remaining_secs)}. killing all jobs:{", ".join(job_names)}')
        jobs_2_kill = [job_ids.get((job_name, 'R'), []) for job_name in job_names]

        for job in job_ids:
            if job_ids[job] == 'R':
                LOGGER.info(f'> {job} = {len(job_ids[job])}')

        LOGGER.debug(f'job_ids = {job_ids}')
        LOGGER.debug(f'jobs_2_kill = {jobs_2_kill}')

        for main_job in jobs_2_kill:
            for job_id in main_job:
                LOGGER.debug(f'[{main_job}] killing {job_id}')
                flux_kill(job_id, signal)

    while True:
        # ---------------------------------------------------------------------------
        # MONITOR by getting flux jobs status on interval and outputing a CSV Row
        ts = strftime("%Y%m%d-%H%M%S")
        job_ids = {}
        get_log_time = 0
        try:
            start_time = time.time()

            # status from flux log
            flux_log = check_output('flux jobs --count=1000000 -n', shell=True, encoding='utf8')

            get_log_time = time.time() - start_time
            
            LOGGER.info(f'flux jobs subprocess took {get_log_time:.3f} seconds ')

            job_ids = flux_parse_log(flux_log, LOGGER)

            # write status into csv row
            LOGGER.info(f'Updating ({STATUS_FILEPATH})')
            status_row = \
                [JOB_ID, NNODES, ts] + \
                [str(len(job_ids[name, 'R'])) for name in config['JOB_NAMES_2_TRACK']] + \
                [str(len(job_ids[name, stat])) for (name, stat) in product(config['JOB_NAMES_2_COUNT'], FLUX_STATUS_CODES)]

            open(STATUS_FILEPATH, 'a').write(','.join(status_row) + '\n')

        except Exception as e:
            LOGGER.error(e)
            # LOGGER.error(traceback.format_exc())
            pass
            get_log_time = 0

        # --------------------------------------------------------------------
        # check if batch job time is short and kill processes and exit if necessary
        remaining_secs = endtime - time.time()

        LOGGER.info(f'Remaining secs ({remaining_secs})')
        LOGGER.info(f'Remaining secs - get_log_time ({remaining_secs-get_log_time})')

        if (remaining_secs - get_log_time) < config['FIRST_KILL_CLEANUP_SECS'] and not wf_signal_sent:
            kill_all_jobs(config['FIRST_KILL_NAMES'])
            wf_signal_sent = True

        if (remaining_secs - get_log_time) < config['SECOND_KILL_CLEANUP_SECS']:
            kill_all_jobs(config['SECOND_KILL_NAMES'])

            LOGGER.info('exiting mummi monitor')
            exit()

        # We write additional logs for better understanding
        try:
            start_time = time.time()

            flux_log_all = check_output(f'flux jobs -R --count=1000000 -o "{flux_format_045}"', shell=True, encoding='utf8')
            # We also save the free resources/allocated etc at each timestamp
            flux_resource_all = check_output(f'flux resource list', shell=True, encoding='utf8')

            # We also output JSON for better processing later
            ps = subprocess.Popen(f'flux jobs --count=1000000 --json | jq', encoding="utf8", shell=True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            flux_log_all_json = ps.communicate()[0]

            LOGGER.info(f'additional flux jobs subprocess took {time.time() - start_time:.3f} seconds ')

            # write raw flux log
            raw_flux_filepath = f'{RAW_FLUX_LOGS_DIR}/flux_log_{ts}.txt'
            LOGGER.info(f'Writing ({raw_flux_filepath})')
            open(raw_flux_filepath, 'w').write(flux_log_all)

            # write raw flux log JSON
            json_flux_filepath = f'{JSON_FLUX_LOGS_DIR}/flux_log_{ts}.json'
            LOGGER.info(f'Writing ({json_flux_filepath})')
            open(json_flux_filepath, 'w').write(flux_log_all_json)

            # write raw flux log
            res_flux_filepath = f'{RES_FLUX_LOGS_DIR}/flux_resource_{ts}.txt'
            LOGGER.info(f'Writing ({res_flux_filepath})')
            open(res_flux_filepath, 'w').write(flux_resource_all)

        except Exception as e:
            LOGGER.error(e)
            pass

        sleep(config['INTERVAL_SECS'])


# ------------------------------------------------------------------------------
def get_endtime_lsf(job_id: str, logger) -> float:
    """
    Return endtime as seconds since epoch.
    """

    job_id_found = False
    time_started = None
    runtime_secs = None
    lines = iter(check_output("bjobs -l", shell=True, encoding='utf8').splitlines())

    for line in lines:
        # skip lines until we find job_id
        if not job_id_found: 
            if line.startswith('Job <'):
                bjob_id = line[5:line.find('>')]
                if  bjob_id == job_id: 
                    job_id_found = True

        # get started time since epoch
        elif line.find('Started') != -1:
            time_str = line[line.find(' ') + 1:line.rfind(':')]
            year_str = str(time.localtime()[0])
            time_started = time.mktime(time.strptime(f'{time_str} {year_str}', '%b %d %H:%M:%S %Y'))

        # get runtime in seconds
        elif line.startswith(' RUNLIMIT'):
            line = next(lines)
            runtime_secs = int(line[:line.find('.')]) * 60
            break
    return  time_started + runtime_secs if time_started and runtime_secs else inf

# ------------------------------------------------------------------------------
def get_endtime_slurm(job_id: str, logger) -> float:
    """
    Return endtime as seconds since epoch.
    """

    job_id_found = False
    endtime = None
    lines = iter(check_output("squeue -u $USER --noheader -o '%A %e'", shell=True, encoding='utf8').splitlines())

    for line in lines:
        data = line.split(' ')
        if not job_id_found: 
            if  data[0] == job_id: 
                job_id_found = True
                endtime = time.mktime(time.strptime(f'{data[1]}',  '%Y-%m-%dT%H:%M:%S'))
                logger.debug(f"data = {data} | endtime {endtime}")
                break

    return endtime if endtime else inf


# ------------------------------------------------------------------------------
def flux_cancel(flux_id: str):
    """
    Cancel a job which means that Flux will send SIGTERM 
    to the job and then 5 sec later a SIGKILL is going to be send.
    WARNING: do not use that to kill a job that has not failed as it will not
    have enought time to checkpoint.
    """
    run(f'flux job cancel {flux_id}', shell=True)

# ------------------------------------------------------------------------------
def flux_kill(flux_id: str, signal: str = "SIGTERM"):
    """Send a signal to a job (by default SIGTERM)"""
    run(f'flux job kill -s {signal} {flux_id}', shell=True)

def flux_parse_log(flux_log: str, logger) -> Dict[Tuple[str, str], List[str]]:
    """
    Read in flux log job_ids into dictionary grouped by name and status
    """

    job_ids = {(name, stat): []
               for (name, stat) in product(config['JOB_NAMES_2_COUNT'] + config['JOB_NAMES_2_TRACK'],
                                           FLUX_STATUS_CODES)}
    
    for line in flux_log.split('\n'):
        if len(line) == 0: continue

        flux_job_id = line.split()[0]
        # _user = line.split()[1]
        name = line.split()[2]
        stat = line.split()[3]
        # _ntasks = line.split()[4]
        # _nnodes = line.split()[5]
        # _runtime = line.split()[6]
        # _host = None
        # if len(line) > 7:
        #     _host = line.split()[7]

        if name in config["JOB_NAMES_2_COUNT"] + config["JOB_NAMES_2_TRACK"]:
            if not stat in FLUX_STATUS_CODES:
                logger.warning(f"Ignored {name} because code {stat} is not in FLUX_STATUS_CODES = {FLUX_STATUS_CODES}")
            else:
                job_ids[name, stat].append(flux_job_id)
        else:
            logger.warning(f"{name} is not in {config['JOB_NAMES_2_COUNT']} or {config['JOB_NAMES_2_TRACK']}")

    return job_ids


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":

    # get config from argparse
    parser = argparse.ArgumentParser(prog='monitor_mummi')
    parser.add_argument('config_filepath', help="filepath to config file"),
    filepath = parser.parse_args(sys.argv[1:]).config_filepath
    config = yaml.load(open(filepath), Loader=yaml.FullLoader)

    main(config)

# ------------------------------------------------------------------------------
