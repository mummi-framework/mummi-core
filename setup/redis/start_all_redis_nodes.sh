#!/usr/bin/env bash
# ------------------------------------------------------------------------------

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

HAS_ERROR=0
re='^[0-9]+$'
if ! [[ $1 =~ $re ]] ; then
    echo "ERROR: must pass MUMMI_REDIS_NNODES to script"; HAS_ERROR=1;
else 
    export MUMMI_REDIS_NNODES=$1
fi
if [ -z "${MUMMI_CORE}" ] ; then
    echo "ERROR: MUMMI_CORE is not set."; HAS_ERROR=1;
fi
if [ -z "${FLUX_URI}" ] ; then
    echo "ERROR: FLUX_URI is not set. Please make sure you are running in a flux instance"; HAS_ERROR=1;
fi

if [ $HAS_ERROR -eq 0 ]; then
    echo "----> Launching Redis nodes (NNODES=${MUMMI_REDIS_NNODES})"
    source $SCRIPT_DIR/load_redis_env.sh
    # source $SCRIPT_DIR/../utils/create_backup_of_directory.sh $MUMMI_REDIS_OUTPUTS
    source $SCRIPT_DIR/create_redis_dirs.sh

    PORTS=`seq 6334 $((6333 + MUMMI_REDIS_NNODES))`
    for PORT in $PORTS;
    do
        echo "> Starting redis node at port" $PORT
        STDOUT="${MUMMI_REDIS_OUTPUTS}/redis_${PORT}.stdout"
        flux mini submit --job-name redis --output=$STDOUT -n1 -N1 -c24 flux start $SCRIPT_DIR/start_redis_node.sh $PORT
        sleep 10s
    done
else
    echo "Failed to launch all redis nodes";
fi

# ------------------------------------------------------------------------------
