# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
# this script is used to create and append to a csv file the curent counts of
# flux jobs. Creates a header of the fields formed from the product of job names and status codes
# in the parameters just below.
# every interval a 'flux jobs' is called and a new CSV row of the job-status counts is added 

# in each interval the endtime amount of remaining time is also checked to allow for cleanup to be performed


from typing import List, Tuple, Dict
from itertools import product
from subprocess import check_output, run
import time
from time import sleep, strftime
from os import makedirs, getenv as env
from os.path import exists
from math import inf
import logging
import sys, argparse, yaml

# ------------------------------------------------------------------------------
FLUX_STATUS_CODES = ['R', 'PD', 'CA', 'CD', 'F', 'TO']
JOB_ID = env('LSB_JOBID', '')
NNODES = env('MUMMI_NNODES', '')
MUMMI_ROOT = env("MUMMI_ROOT")
MONITOR_DIRPATH   = f'{MUMMI_ROOT}/monitoring'
STATUS_FILEPATH   = f'{MONITOR_DIRPATH}/status_{strftime("%Y%m%d-%H%M%S")}.csv'
RAW_FLUX_LOGS_DIR = f'{MONITOR_DIRPATH}/raw_flux_logs'


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

    # write CSV header
    header_columns = \
        ['JOB_ID', 'NNODES', 'timestamp'] + \
        [f'{name}' for name in config['JOB_NAMES_2_TRACK']] + \
        [f'#{stat}_{name}' for name, stat in product(config['JOB_NAMES_2_COUNT'], FLUX_STATUS_CODES)]
    open(STATUS_FILEPATH, 'w').write(','.join(header_columns) + '\n')

    # calculate endtime of job
    endtime = get_endtime(JOB_ID)
    if endtime == inf:
        LOGGER.warning('MONITOR > job endtime failed to calculate. jobs will not be killed!')

    wf_signal_sent = False

    def kill_all_jobs(job_names):
        LOGGER.info(f'MONITOR > time remaining(sec): {int(remaining_secs)}. killing all jobs:{", ".join(job_names)}')
        for job_id in [job_ids.get((job_name, 'R'), []) for job_name in job_names]:
            flux_cancel(job_id)

    while True:

        # ---------------------------------------------------------------------------
        # MONITOR by getting flux jobs status on interval and outputing a CSV Row
        ts = strftime("%Y%m%d-%H%M%S")
        job_ids = {}

        try:
            start_time = time.time()

            # status from flux log
            flux_log = check_output('flux jobs -a --count=1000000', shell=True, encoding='utf8')
            LOGGER.info(f'flux jobs subprocess took {time.time() - start_time:.3f} seconds ')

            job_ids = flux_parse_log(flux_log)

            # write raw flux log
            raw_flux_filepath = f'{RAW_FLUX_LOGS_DIR}/flux_log_{ts}.txt'
            LOGGER.info(f'Writing ({raw_flux_filepath})')
            open(raw_flux_filepath, 'w').write(flux_log)

            # write status into csv row
            LOGGER.info(f'Updating ({STATUS_FILEPATH})')
            status_row = \
                [JOB_ID, NNODES, ts] + \
                [str(len(job_ids[name, 'R'])) for name in config['JOB_NAMES_2_TRACK']] + \
                [str(len(job_ids[name, stat])) for (name, stat) in product(config['JOB_NAMES_2_COUNT'], FLUX_STATUS_CODES)]

            open(STATUS_FILEPATH, 'a').write(','.join(status_row) + '\n')

        except Exception as e:
            pass
            LOGGER.error(e)

        # --------------------------------------------------------------------
        # check if batch job time is short and kill processes and exit if necessary
        remaining_secs = endtime - time.time()

        if remaining_secs < config['FIRST_KILL_CLEANUP_SECS'] and not wf_signal_sent:
            kill_all_jobs(config['FIRST_KILL_NAMES'])
            wf_signal_sent = True

        if remaining_secs < config['SECOND_KILL_CLEANUP_SECS']:
            kill_all_jobs(config['SECOND_KILL_NAMES'])

            LOGGER.info('exiting mummi monitor')
            exit()

        sleep(config['INTERVAL_SECS'])


# ------------------------------------------------------------------------------
def get_endtime(job_id: str) -> float:
    """
    Return endtime as seconds since epoch.
    """

    job_id_found = False
    time_started = None
    runtime_secs = None
    lines = iter(check_output('bjobs -l', shell=True, encoding='utf8').splitlines())

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
def flux_cancel(flux_id: str):
    run(f'flux job cancel {flux_id}', shell=True)


def flux_parse_log(flux_log: str) -> Dict[Tuple[str, str], List[str]]:
    """
    Read in flux log job_ids into dictionary grouped by name and status
    """

    job_ids = {(name, stat): []
               for (name, stat) in product(config['JOB_NAMES_2_COUNT'] + config['JOB_NAMES_2_TRACK'],
                                           FLUX_STATUS_CODES)}

    for line in flux_log.split('\n'):
        if len(line) == 0: continue

        flux_job_id, _user, name, stat, _ntasks, _nnodes, _runtime, _ranks = line.split()

        if name in config['JOB_NAMES_2_COUNT'] + config['JOB_NAMES_2_TRACK']:
            job_ids[name, stat].append(flux_job_id)

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
