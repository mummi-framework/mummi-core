#!/usr/bin/env bash
# ------------------------------------------------------------------------------

echo '> Loading redis common env'

if [ -n "${MUMMI_ROOT}" ] ; then
    echo 'MUMMI_ROOT: '$MUMMI_ROOT
    export MUMMI_REDIS_SERVER=`which redis-server`
    export MUMMI_REDIS_CLIENT=`which redis-cli`
    export MUMMI_REDIS_OUTPUTS=$MUMMI_ROOT/redis
else
    echo "ERROR: MUMMI_ROOT is not set. Please set and retry";
fi

# ------------------------------------------------------------------------------
