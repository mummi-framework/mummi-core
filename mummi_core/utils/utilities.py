# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import datetime
import logging
import os
import shutil
import subprocess
import multiprocessing
import time
import sys
import traceback
import signal

LOGGER = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
def sig_ign_and_rename_proc(new_proc_name):
    
    proc =  multiprocessing.current_process()

    # attept to add the pool process rank to name
    proc_rank = ''
    try:
        proc_rank = str(proc._identity[0])
    except : pass

    proc.name = f"{new_proc_name}:{proc_rank}:{os.getpid()}"
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)


# ------------------------------------------------------------------------------
def get_memory_usage(process=None):
    import psutil
    if process is None:
        process = psutil.Process(os.getpid())

    bytes = float(process.memory_info().rss)
    if bytes < 1024.0:
        return f"{bytes:d} bytes"

    kb = bytes / 1024.0
    if kb < 1024.0:
        return f"{kb:.3f} KB"

    mb = kb / 1024.0
    if mb < 1024.0:
        return f"{mb:.3f} MB"

    gb = mb / 1024.0
    return f"{gb:.3f} GB"


# ------------------------------------------------------------------------------
def silent_remove(file):
    try:
        os.remove(file)
    except OSError as e:
        pass


def silent_rename(src, dest):
    try:
        os.rename(src, dest)
    except OSError as e:
        pass


# ------------------------------------------------------------------------------
def remove_parallel(files, nprocesses = 4):

    p = multiprocessing.Pool(nprocesses)
    try:
        p.map(silent_remove, files)
    except Exception as e:
        LOGGER.error('Failed to cleanup keys using process pool')
    finally:
        p.close()


# ------------------------------------------------------------------------------
#ACCEPTED_INPUT = set(["yes", "y"])
def partition_list(data, flags):
    assert isinstance(data, list) and isinstance(flags, list)
    assert len(data) == len(flags)
    assert all([isinstance(_, bool) for _ in flags])

    _true, _false = [], []
    for i,v in enumerate(data):
        if flags[i]:
            _true.append(v)
        else:
            _false.append(v)

    return _true, _false


# ------------------------------------------------------------------------------
def read_arg(x):
    if hasattr(x, "__getitem__"):   return x[0]
    else:                           return x


def format_time(ts):
    if ts == None:
        return ts
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

# ------------------------------------------------------------------------------
def expand_path(p):
    p = os.path.expandvars(p)           # expands environment variables
    if p[0] == '~':
        p = os.path.expanduser(p)       # expands home directory
    return os.path.abspath(p)


def mkdir(path):
    if os.path.isdir(path):
        return

    try:
        #oldumask = os.umask(0)
        #os.makedirs(path, mode=0o770)
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno != errno.EEXIST or not os.path.isdir(path):
            raise


def get_file(fullName, name):
    """
    Get the indicated file.

      - Currently copy file to current dir
      - Later maybe move to data broker
    """
    check_file(fullName)
    shutil.copy(fullName, name)


def check_file(fullName):
    """Check if file exists - throws error is not."""
    if not os.path.isfile(fullName):
        error = "File not found: "+fullName
        LOGGER.error(error)
        raise NameError(error)


def add_timestamp(name):
    """Add timestamp to name."""
    return '{0}_{1}'.format(name, time.strftime("%Y%m%d-%H%M%S"))


def sys_call(command, test=False, run_dir="", return_codes=[0]):
    """
    Call system function or print.

    If in test mode set test to default True for print only and False for
    executing commands. (WARNING a few e.g. make dir are set always to False)
    """
    if test:
        LOGGER.info(command)
        return 0
    else:
        if run_dir != "":
            p = subprocess.Popen(command, shell=True, cwd=run_dir)
        else:
            p = subprocess.Popen(command, shell=True)
        retcode = p.wait()
        if retcode not in return_codes:
            LOGGER.debug("Failed run: " + command)
            sys.exit(retcode)
        else:
            LOGGER.info("Run command: " + command)
            return 0


def timeout(seconds: int):
    '''
    Timeout decorator acts as a timout and a try/catch block around the function

    : param seconds:  the number of seconds till timeout

    Example usage:

        @timeout(60)
        def my_function(my_param1: int, my_parma2: str) -> int:
            sleep(200)
            return 5
    '''

    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
            parent_conn, child_conn = multiprocessing.Pipe()
            p = multiprocessing.Process(target=subprocess, 
                args=(child_conn, func, args, kwargs))
            p.start()
            init_time = time.time()
            while time.time() - init_time <= seconds:
                if p.is_alive():
                    time.sleep(0.00001) # ping faster than 0.01 msec doesn't speed up process
                else:
                    out = None
                    try:
                        out = parent_conn.recv()
                    except:
                        pass
                    p.join()
                    return out
            else:
                LOGGER.error('{}() timed out after {} seconds'.format(func.__name__, seconds))
                p.terminate()

        def subprocess(conn, func, args, kwargs):
            out = None
            try:
                out = func(*args, **kwargs)
            except Exception as e:
                LOGGER.error('{}() failed'.format(func.__name__))
                traceback.print_exc()
            
            conn.send(out)
            conn.close()
            return out

        return wrapper

    return decorator

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
