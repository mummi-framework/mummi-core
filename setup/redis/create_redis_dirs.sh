#!/usr/bin/env bash
# ------------------------------------------------------------------------------

if [ -z "${MUMMI_ROOT}" ] ; then
    echo "ERROR: MUMMI_ROOT is not set. Please set and retry";
else
    echo '> Creating redis directories in ${MUMMI_ROOT}/redis'
    # rm -rf $MUMMI_REDIS_OUTPUTS
    rm -rf $MUMMI_REDIS_OUTPUTS/all_servers*
    mkdir -p $MUMMI_REDIS_OUTPUTS
fi

# ------------------------------------------------------------------------------
