#!/usr/bin/env bash
# ------------------------------------------------------------------------------

HAS_ERROR=0
re='^[0-9]+$'
if ! [[ $1 =~ $re ]] ; then
    echo "ERROR: must pass port to script"; HAS_ERROR=1;
else 
    export PORT=$1
fi
if [ -z "${MUMMI_REDIS_OUTPUTS}" ] ; then
    echo "ERROR: MUMMI_REDIS_OUTPUTS is not set. Please source setup/redis/load_redis_env.sh"; HAS_ERROR=1;
fi
if [ -z "${FLUX_URI}" ] ; then
    echo "ERROR: FLUX_URI is not set. Please make sure you are running in a flux instance"; HAS_ERROR=1;
fi

if [ $HAS_ERROR -eq 0 ]; then
    echo 'Starting redis node'
    echo 'MUMMI_REDIS_OUTPUTS: '$MUMMI_REDIS_OUTPUTS
    echo 'PORT: '$PORT
    mummi_bind_global_redis `hostname` $PORT
    $MUMMI_REDIS_SERVER --protected-mode no --port $PORT --dir $MUMMI_REDIS_OUTPUTS --loglevel verbose --appendonly yes --appendfsync everysec
else
    echo "Failed to launch redis node";
fi

# ------------------------------------------------------------------------------
