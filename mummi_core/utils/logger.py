# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import os
import sys
import time
import threading
import multiprocessing
import logging

from .utilities import expand_path
from .utilities import get_memory_usage


# ------------------------------------------------------------------------------
# MuMMI logging utilities
# ------------------------------------------------------------------------------
# string formats for logging and history
MUMMI_FMT_LOG = '%(asctime)s - [%(processName)s:%(process)d] %(name)s:%(funcName)s:%(lineno)s - %(levelname)s - %(message)s'
MUMMI_FMT_HIST = '%(asctime)s - %(name)s:%(funcName)s:%(lineno)s - %(message)s'


# ------------------------------------------------------------------------------
def append_mem_usage(message):
    return f"[{get_memory_usage()}]: {message}"


def _log_debug_with_memory(self, message, *args, **kws):
    self._log(logging.DEBUG, append_mem_usage(message), args, **kws)


def _log_info_with_memory(self, message, *args, **kws):
    self._log(logging.INFO, append_mem_usage(message), args, **kws)


def _log_warning_with_memory(self, message, *args, **kws):
    self._log(logging.WARNING, append_mem_usage(message), args, **kws)


def _log_error_with_memory(self, message, *args, **kws):
    self._log(logging.ERROR, append_mem_usage(message), args, **kws)


def _log_critical_with_memory(self, message, *args, **kws):
    self._log(logging.CRITICAL, append_mem_usage(message), args, **kws)


# ------------------------------------------------------------------------------
# ID for history logging
MUMMI_LOG_HISTORY = logging.CRITICAL + 1


def _log_history(self, message, *args, **kws):
    if self.isEnabledFor(MUMMI_LOG_HISTORY):
        self._log(MUMMI_LOG_HISTORY, message, args, **kws)


logging.addLevelName(MUMMI_LOG_HISTORY, "HISTORY")
logging.Logger.history = _log_history


# ------------------------------------------------------------------------------
# ID for profile logging
MUMMI_LOG_PROFILE = logging.CRITICAL + 2


def _log_profile(self, message, *args, **kws):
    if self.isEnabledFor(MUMMI_LOG_PROFILE):
        self._log(MUMMI_LOG_PROFILE, append_mem_usage(message), args, **kws)


logging.addLevelName(MUMMI_LOG_PROFILE, "PROFILE")
logging.Logger.profile = _log_profile


# ------------------------------------------------------------------------------
# get logging level in "logging" format
def _get_logginglevel(level):
    assert 1 <= level <= 5
    if level == 1:  return logging.DEBUG
    if level == 2:  return logging.INFO
    if level == 3:  return logging.WARN
    if level == 4:  return logging.ERROR
    if level == 5:  return logging.CRITICAL


# parse the filename and path for the logging file
def _get_logfilename(do_file):

    if do_file == '':
        return '.', ''

    do_file = expand_path(do_file)
    if os.path.isdir(do_file):
        return do_file, 'log.log'

    # else
    pname = os.path.dirname(do_file)
    fname = os.path.basename(do_file)

    if fname[-4:] != '.log':
        fname += '.log'

    return pname, fname
    logfile = os.path.join(pname, fname)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class MultiprocessFileHandler(logging.FileHandler):
    """
    Multithreaded queue that writes logs to workspace file.
    """
    def __init__(self, filename, backup_freq=30):
        logging.FileHandler.__init__(self, filename)

        self.backup_freq = backup_freq
        self.queue = multiprocessing.Queue(-1)

        try:
            thread = threading.Thread(target=self.receive)
            thread.daemon = True
            thread.start()
        except (KeyboardInterrupt, SystemExit):
            sys.exit()

    def receive(self):
        backup_time = time.time()
        while True:
            try:
                # Pop queue and actually emit the record
                for i in range(100):
                    if self.queue.empty(): break
                    record = self.queue.get()
                    logging.FileHandler.emit(self, record)

                # Backup log file
                if (time.time() - backup_time > self.backup_freq):
                    if os.path.exists(self.baseFilename):
                        shutil.copyfile(self.baseFilename, self.baseFilename + '.bak')
                        LOGGER.info('Created backup of log file {}'.format(self.baseFilename))
                    backup_time = time.time()
            except EOFError:
                break
            except Exception as e:
                traceback.print_exc(file=sys.stderr)
                raise e
            time.sleep(1)

    # Instead of actually emitting record, put it into queue
    def emit(self, record):
        try:
            self.queue.put(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self.handleError(record)


# ------------------------------------------------------------------------------
class NoHistoryFilter(logging.Filter):
    def filter(self, record):
        return record.levelno != MUMMI_LOG_HISTORY


class HistoryFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == MUMMI_LOG_HISTORY


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
def init_logger(**kwargs):

    #for key, value in kwargs.items():
    #    print("{0} = {1}".format(key, value))

    # --------------------------------------------------------------------------
    # if a dictionary of config has been given, overwrite the values
    # this is useful to parse mummi's specs
    if 'config' in kwargs:

        from mummi_core import Naming

        lconfig = kwargs['config']

        level = int(lconfig['loglevel'])
        do_stdout = bool(lconfig['logstdout'])
        do_file = bool(lconfig['logfile'])
        do_mem_usage = bool(lconfig["log_mem_usage"])
        do_history = False

        if not do_file:
            do_file = ''

        else:
            logpath = str(lconfig['logpath'])
            if len(logpath) == 0:
                logpath = Naming.dir_root('workspace')
            do_file = os.path.join(logpath, f"{lconfig['jobname']}.log")

    # If argparser set, used in e.g. creatsims and cg_analysis
    elif 'argparser' in kwargs:
        largs = kwargs['argparser']

        level = int(largs.loglevel)
        do_stdout = bool(largs.logstdout)
        do_file = os.path.join(str(largs.logpath), f'{str(largs.logfile)}.log')
        do_history = False
        do_mem_usage = False

    else:
        level = int(kwargs.get('level', 2))
        do_stdout = bool(kwargs.get('stdout', True))
        do_file = str(kwargs.get('file', ''))
        do_history = bool(kwargs.get('history', False))
        do_mem_usage = bool(kwargs.get("mem_usage", False))

    print(f'> Initializing logger:  level = ({level}), '
          f'stdout = ({do_stdout}), file = ({do_file}), '
          f'history = ({do_history}),  memusage = ({do_mem_usage})')

    # --------------------------------------------------------------------------
    # --------------------------------------------------------------------------
    level = _get_logginglevel(level)
    pname, fname = _get_logfilename(do_file)

    logger = logging.getLogger()
    logger.setLevel(level)

    # --------------------------------------------------------------------------
    # create a stream handler
    if do_stdout:
        sh = logging.StreamHandler()
        sh.setLevel(level)
        sh.setFormatter(logging.Formatter(MUMMI_FMT_LOG))
        sh.addFilter(NoHistoryFilter())
        logger.addHandler(sh)

        logger.debug('Logging enabled: stdout')

    # --------------------------------------------------------------------------
    # create a file handler if needed
    if do_file != '':

        if pname != '.' and not os.path.exists(pname):
            os.makedirs(pname)

        logfile = os.path.join(pname, fname)

        # ------------------------------------------------------------------
        #fh = MultiprocessFileHandler(logfile)
        fh = logging.FileHandler(logfile)
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter(MUMMI_FMT_LOG))
        fh.addFilter(NoHistoryFilter())
        logger.addHandler(fh)

        logger.debug('Logging enabled: file ({})'.format(logfile))

    # --------------------------------------------------------------------------
    # if we want to show the memory usage
    if do_mem_usage:
        logging.Logger.info = _log_info_with_memory
        logging.Logger.debug = _log_debug_with_memory
        logging.Logger.warning = _log_warning_with_memory
        logging.Logger.error = _log_error_with_memory
        logging.Logger.critical = _log_critical_with_memory

    # --------------------------------------------------------------------------
    # now, create a history logger
    if do_history:

        if pname != '.' and not os.path.exists(pname):
            os.makedirs(pname)

        # ------------------------------------------------------------------
        # figure out the history filename
        idx = 0
        histfile = os.path.join(pname, 'history_{}.log'.format(idx))
        while os.path.exists(histfile):
            idx += 1
            histfile = os.path.join(pname, 'history_{}.log'.format(idx))

        # ------------------------------------------------------------------
        #hh = MultiprocessFileHandler(histfile)
        hh = logging.FileHandler(histfile)
        hh.setLevel(level)
        hh.setFormatter(logging.Formatter(MUMMI_FMT_HIST))
        hh.addFilter(HistoryFilter())
        logger.addHandler(hh)

        logger.debug('History enabled: file ({})'.format(histfile))

    # --------------------------------------------------------------------------
    # Print the level of logging.
    logger.debug('Enabled')
    logger.info('Enabled')
    logger.warning('Enabled')
    logger.error('Enabled')
    logger.critical('Enabled')
    logger.history('Enabled')
    logger.profile('Enabled')

# ------------------------------------------------------------------------------
