#!/usr/bin/env bash
# ------------------------------------------------------------------------------
#BSUB -P pbatch

# ------------------------------------------------------------------------------
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

NNODES=$1
FLUX_ROOT=$(realpath $2)

FLUX_INFO=$FLUX_ROOT/flux.info
FLUX_LOG=$FLUX_ROOT/flux.log
FLUX_BOOTSTRAP=$SCRIPT_DIR/flux_bootstrap.sh
FLUX_ENV=$SCRIPT_DIR/flux_env.sh

re='^[0-9]+$'
if ! [[ $NNODES =~ $re ]] ; then
   echo "ERROR: Nodes is not a number" >&2; exit 1
fi

source $FLUX_ENV
# ------------------------------------------------------------------------------
echo ''
echo '----> Launching Flux'
echo " > flux   : `which flux`"
echo " > NUM_NODES = $NNODES"
echo " > FLUX_ROOT = $FLUX_ROOT"
echo " > FLUX_INFO = $FLUX_INFO"
echo " > FLUX_SSH  = $FLUX_SSH"

# ------------------------------------------------------------------------------

echo " > Launching Flux using jsrun"

mkdir -p $FLUX_ROOT
cd $FLUX_ROOT

# --smpiargs="-disable_gpu_hooks"
unset OMP_NUM_THREADS
# from open_source branch tested on lassen
#env PMIX_MCA_gds="^ds12,ds21"
# campaign4 code from summit
env FLUXION_QMANAGER_OPTIONS='queue-params=queue-depth=32' FLUXION_RESOURCE_OPTIONS="load-allowlist=node,core,gpu prune-filters=ALL:core,ALL:gpu reserve-vtx-vec=2000000 policy=first" jsrun -a 1 -c ALL_CPUS -g ALL_GPUS --bind=none -n $NNODES flux start -o,-S,log-filename=$FLUX_LOG -v $FLUX_BOOTSTRAP $FLUX_INFO &

#echo " > Flux launched using jsrun"
#echo " > flux modules: "
#echo "`flux module list`"
#echo " > version:"
#echo "`flux version`"

# ------------------------------------------------------------------------------
# Saved version of above line from old repo:
#PMI_LIBRARY=$PMI jsrun -a 1 -c ALL_CPUS -g ALL_GPUS --bind=none -n $1 $SCRIPTDIR/env_clean.sh $FLUX start -o,-S,log-filename=flux.log,--setattr=persist-directory=$FLUX_ROOT $SCRIPTDIR/ip.sh

# ------------------------------------------------------------------------------
