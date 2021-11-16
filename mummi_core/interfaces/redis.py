# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import os
import io
import random
import redis
from logging import getLogger
from filelock import FileLock
from collections import defaultdict

from mummi_core.utils import Naming
from .base import IO_Base
from .default_functions import read_npz

LOGGER = getLogger(__name__)


# ------------------------------------------------------------------------------
# Tar Interface
# ------------------------------------------------------------------------------
class IO_Redis (IO_Base):

    ## TODO: should be configurable
    TMP_DIR = '/var/tmp/mummi'
    LOCAL_SERVER_TXT = os.path.join(TMP_DIR, 'server.txt')
    ALL_SERVERS_TXT = os.path.join(Naming.dir_root('redis'), 'all_servers.txt')

    # --------------------------------------------------------------------------
    # Public Abstract functions
    # --------------------------------------------------------------------------
    @classmethod
    def get_type(cls):
        return 'redis'

    @classmethod
    def check_environment(cls):
        return cls.bind_local_redis()

    @classmethod
    def file_exists(cls, namespace, key):
        assert isinstance(namespace, str) and isinstance(key, str)
        redis_key = cls._format_redis_key(namespace, key)
        try:
            conns = cls._get_remote_connections()
            for conn in conns:
                if conn.exists(redis_key):
                    return True
        except Exception as e:
            LOGGER.error(f'Failed to check file exists: {e}')
        return False

    @classmethod
    def namespace_exists(cls, namespace):
        raise Exception('IO_Redis does not have namespace_exists')
        # assert isinstance(namespace, str)
        # return len(cls.list_keys(namespace, '*')) > 0

    # --------------------------------------------------------------------------
    # Private Abstract functions
    # --------------------------------------------------------------------------
    @classmethod
    def _list_keys(cls, namespace, keypattern):
        servers_to_keys = cls.list_servers_to_keys(namespace, keypattern)
        return [k for d in servers_to_keys.values() for k in d]

    @classmethod
    def _move_key(cls, namespace, old, new):
        raise Exception('IO_Redis cannot currently move keys')

    @classmethod
    def _load_files(cls, namespace, keys):

        data = [None] * len(keys)
        idxs = {k: v for v, k in enumerate(keys)}
        remaining_keys = keys
        servers = cls._get_all_servers()
        for server in servers:
            keys_to_data = cls._load_files_at_server(namespace, remaining_keys, server)
            remaining_keys = [k for k in remaining_keys if not k in keys_to_data]
            for k in keys_to_data:
                data[idxs[k]] = keys_to_data[k]
        LOGGER.debug(f'Loaded {len(keys)-len(remaining_keys)} out of {len(keys)} ' +
                     f'keys across {len(servers)} servers')
        return data

    @classmethod
    def _load_files_at_server(cls, namespace, keys, server):
        try:
            keys_to_data = {}
            conn = IO_Redis._get_remote_connection(server)
            for key in keys:
                redis_key = cls._format_redis_key(namespace, key)
                if conn.exists(redis_key):
                    keys_to_data[key] = conn.get(redis_key)
            return keys_to_data
        except Exception as e:
            LOGGER.error(f'Failed to load files at {server}: {e}')

    @classmethod
    def _save_files(cls, namespace, keys, data):

        LOGGER.debug(f'Writing {len(keys)} files to ({namespace})')
        try:
            conn = IO_Redis._get_local_connection()
            for i, fname in enumerate(keys):
                redis_key = cls._format_redis_key(namespace, fname)
                d = cls._encode(data[i])
                conn.set(redis_key, d)
            LOGGER.info(f'Wrote {len(keys)} files to server {conn.connection_pool.connection_kwargs["host"]}')
            return True
        except Exception as e:
            LOGGER.error(f'Failed to save files: {e}')
            return False

    @classmethod
    def _remove_files(cls, namespace, keys):

        remaining_keys = keys
        servers = cls._get_all_servers()
        for server in servers:
            keys_to_data = cls.remove_files_at_server(namespace, remaining_keys, server)
            remaining_keys = [k for k in remaining_keys if not k in keys_to_data]

        LOGGER.debug(f'Removed {len(keys) - len(remaining_keys)} out of {len(keys)} keys ' +
                     f'across {len(servers)} servers')

    @classmethod
    def remove_files_at_server(cls, namespace, keys, server):
        try:
            conn = IO_Redis._get_remote_connection(server)
            count = 0
            for key in keys:
                redis_key = cls._format_redis_key(namespace, key)
                if conn.exists(redis_key):
                    conn.delete(redis_key)
                    count += 1
            LOGGER.debug(f'Deleted {count} out of {len(keys)} keys at {server}')
        except Exception as e:
            LOGGER.error(f'Failed to delete keys in {namespace} at {server}: {e}')

    @classmethod
    def rename_files_at_server(cls, old_namespace, new_namespace, keys, server):

        try:
            conn = IO_Redis._get_remote_connection(server)
            count = 0
            for key in keys:
                old_key = cls._format_redis_key(old_namespace, key)
                new_key = cls._format_redis_key(new_namespace, key)
                if conn.exists(old_key):
                    conn.rename(old_key, new_key)
                    count += 1
            LOGGER.info(f'Renamed {count} out of {len(keys)} keys '
                        f'from {old_namespace} to {new_namespace} at {server}')
        except Exception as e:
            LOGGER.error(f'Failed to rename keys in {old_namespace} at {server}: {e}')

    # --------------------------------------------------------------------------
    # IO_Redis Public Functions
    # --------------------------------------------------------------------------
    @classmethod
    def bind_global_redis(cls, hostname, port):
        wspace_redis = Naming.dir_root('redis')

        os.makedirs(wspace_redis, exist_ok=True)
        lock = FileLock(f"{cls.ALL_SERVERS_TXT}.lock")
        try:
            with lock.acquire(timeout=10):
                open(cls.ALL_SERVERS_TXT, "a").write(f"{hostname} {port}\n")
        except Exception as e:
            LOGGER.error(f'Failed to bind to global server list: {e}')
            raise e

    @classmethod
    def bind_local_redis(cls):
        wspace_redis = Naming.dir_root('redis')

        if not os.path.isfile(cls.ALL_SERVERS_TXT):
            # LOGGER.warning(f'Could not find server file {cls.ALL_SERVERS_TXT}')
            return False

        all_servers = open(cls.ALL_SERVERS_TXT, 'r').readlines()
        all_servers = [l.strip() for l in all_servers if l]

        os.makedirs(cls.TMP_DIR, exist_ok=True)
        lock = FileLock(f"{cls.LOCAL_SERVER_TXT}.lock")
        try:
            with lock.acquire(timeout=10):

                # Re-use connections to reduce network bandwidth
                reuse_connection = False
                if os.path.exists(cls.LOCAL_SERVER_TXT):
                    line = open(cls.LOCAL_SERVER_TXT, "r").read()
                    if line in all_servers:
                        reuse_connection = True
                        LOGGER.debug(f'Re-using server connection at {line}')

                if not reuse_connection:
                    line = random.choice(all_servers)
                    open(cls.LOCAL_SERVER_TXT, "w").write(line)
        except Exception as e:
            LOGGER.error(f'Failed to bind to local server: {e}')
            return False
        return True

    @classmethod
    def list_servers_to_keys(cls, namespace, keypattern):
        try:
            redis_keypattern = cls._format_redis_key(namespace, keypattern)
            redis_prefix = cls._format_redis_key(namespace, '')
            servers_to_keys = defaultdict(list)
            conns = cls._get_remote_connections()
            for conn in conns:
                hostname = conn.connection_pool.connection_kwargs['host']
                fnames = conn.keys(redis_keypattern)
                for fname in fnames:
                    fname = fname.decode("utf-8")
                    fname = fname.replace(redis_prefix, "", 1)
                    servers_to_keys[hostname].append(fname)
            LOGGER.debug(f'Found {sum([len(d) for d in servers_to_keys.values()])} keys across {len(conns)} servers')
            return servers_to_keys

        except Exception as e:
            LOGGER.error(f'Failed to list keys: {e}')
            return {}

    @classmethod
    def load_npz_at_server(cls, namespace, keys, hostname, reader_func=read_npz):
        keys_to_data = cls._load_files_at_server(namespace, keys, hostname)
        for key in keys_to_data:
            keys_to_data[key] = reader_func(io.BytesIO(keys_to_data[key]))
        return keys_to_data

    # --------------------------------------------------------------------------
    # IO_Redis Private Functions
    # --------------------------------------------------------------------------
    @classmethod
    def _get_local_server(cls):
        with open(cls.LOCAL_SERVER_TXT) as f:
            hostname, port = f.read().strip().split(' ')
        return hostname, port

    @classmethod
    def _get_all_servers(cls):
        servers_to_ports = {}
        with open(cls.ALL_SERVERS_TXT) as f:
            for line in f.readlines():
                hostname, port = line.strip().split(' ')
                servers_to_ports[hostname] = port
        return servers_to_ports

    @classmethod
    def _get_local_connection(cls):
        hostname, port = cls._get_local_server()
        try:
            return redis.Redis(host=hostname, port=port)
        except Exception as e:
            LOGGER.error(f'Failed to connect to local server: {e}')

    @classmethod
    def _get_remote_connection(cls, server):
        servers_to_ports = cls._get_all_servers()
        try:
            for h, p in servers_to_ports.items():
                if h == server:
                    return redis.Redis(host=h, port=p)
            LOGGER.error(f'Failed to open connection. ' +
                         f'Check that server at ({server}) is actually running.')
        except Exception as e:
            LOGGER.error(f'Failed to connect to ({server}): {e}')

    @classmethod
    def _get_remote_connections(cls):
        try:
            servers_to_ports = cls._get_all_servers()
            return [redis.Redis(host=h, port=p) for h, p in servers_to_ports.items()]
        except Exception as e:
            LOGGER.error(f'Failed to connect to servers: {e}')

    @classmethod
    def _format_redis_key(cls, namespace, key):
        return f'{namespace}::{key}'

    @classmethod
    def remove_keys_at_server(cls, namespace, keys, server):
        try:
            conn = IO_Redis._get_remote_connection(server)
            for key in keys:
                redis_key = cls._format_redis_key(namespace, key)
                if conn.exists(redis_key):
                    conn.delete(redis_key)
        except Exception as e:
            LOGGER.error(f'Failed to delete files at {server}: {e}')
    
# ------------------------------------------------------------------------------
