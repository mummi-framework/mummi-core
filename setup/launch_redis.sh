#!/usr/bin/env bash
# ------------------------------------------------------------------------------

mummi_redis_nnodes=$1
re='^[0-9]+$'
if ! [[ $mummi_redis_nnodes =~ $re ]] ; then
  echo "(`hostname`: `date`) --> ERROR: mummi_redis_nodes is not a number" >&2
  exit 1
fi

if [ $mummi_redis_nnodes -eq 0 ]; then
  echo "(`hostname`: `date`) --> ERROR: Not Launching Redis because mummi_redis_nnodes = $mummi_redis_nnodes" >&2
  exit 1
fi

echo "(`hostname`: `date`) --> Launching Redis with $mummi_redis_nnodes nodes"

source $MUMMI_CORE/setup/redis/load_redis_env.sh
source $MUMMI_CORE/setup/redis/start_all_redis_nodes.sh $mummi_redis_nnodes

# ------------------------------------------------------------------------------
