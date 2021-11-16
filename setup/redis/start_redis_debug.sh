#!/usr/bin/env bash
# ------------------------------------------------------------------------------

HAS_ERROR=0
# Pick a random port to avoid risk of collision with suspended Redis processes
# Note: not required with Flux since suspended processes will be terminated
PORT="$(shuf -i 49152-65535 -n 1)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SAVE_FREQUENCY=60 # seconds

if [ $HAS_ERROR -eq 0 ]; then
    source $SCRIPT_DIR/load_redis_env.sh
    source $SCRIPT_DIR/../utils/create_backup_of_directory.sh $MUMMI_REDIS_OUTPUTS
    source $SCRIPT_DIR/create_redis_dirs.sh

    echo 'Starting redis node'
    echo 'MUMMI_REDIS_OUTPUTS: '$MUMMI_REDIS_OUTPUTS
    echo 'PORT: '$PORT
    mummi_bind_global_redis `hostname` $PORT

    # Redis snapshot: faster but less fault tolerant
    # $MUMMI_REDIS_SERVER --protected-mode no --port $PORT --dir $MUMMI_REDIS_OUTPUTS --loglevel verbose --save $SAVE_FREQUENCY 1

    # Redis appendfile: append all changes to file
    $MUMMI_REDIS_SERVER --protected-mode no --port $PORT --dir $MUMMI_REDIS_OUTPUTS --loglevel verbose --appendonly yes --appendfsync everysec
else
    echo "Failed to launch redis node";
fi

# ------------------------------------------------------------------------------
