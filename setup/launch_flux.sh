#!/usr/bin/env bash
# ------------------------------------------------------------------------------

function version() {
  echo "$@" | awk -F. '{ printf("%d%03d%03d%03d\n", $1,$2,$3,$4); }';
}

# the script needs the number of nodes for flux
flux_nnodes=$1
re='^[0-9]+$'
if ! [[ $flux_nnodes =~ $re ]] ; then
  echo "(`hostname`: `date`) --> ERROR: Nodes is not a number" >&2
  exit 1
fi

# ------------------------------------------------------------------------------
echo "(`hostname`: `date`) --> Launching Flux with $flux_nnodes nodes"

# define the variables needed for this script
flux_root=$MUMMI_ROOT/flux
flux_server=$flux_root/flux_server.info
flux_client=$flux_root/flux_client.env
flux_log=$flux_root/flux.log
flux_bootstrap=$MUMMI_CORE/setup/flux/flux_bootstrap.sh

echo "   flux        = $(command -v flux)"
echo "   flux_root   = $flux_root"

# grab the flux environment
source $MUMMI_CORE/setup/flux/flux_env.sh

# -----------------------------------------------------------------------------
# take backup of previous flux logs
if [ -f $flux_info ] ; then
  $MUMMI_APP/setup/utils/create_backup_of_directory.sh $flux_root
  rm -rf $flux_root
fi

mkdir -p $flux_root
pushd $flux_root >/dev/null 2>&1

# ------------------------------------------------------------------------------
unset OMP_NUM_THREADS

if [ -f "$flux_root/content.sqlite" ]; then
  echo "(`hostname`: `date`) --> Found ($flux_root/content.sqlite), deleting it."
  rm -f "$flux_root/content.sqlite" "$flux_root/content.sqlite-wal"
fi

MACHINE=$(echo $(hostname --long) | sed -e 's/[0-9]*$//')
if [[ "$MACHINE" == *"lassen"* ]] ; then

  # spack sets these variable for Flux but they conflict with the installed Flux
  unset FLUX_CONNECTOR_PATH FLUX_EXEC_PATH FLUX_MODULE_PATH FLUX_RC_EXTRA LUA_PATH

  source /etc/profile.d/z00_lmod.sh
  module use /usr/tce/modulefiles/Core
  module use /usr/global/tools/flux/blueos_3_ppc64le_ib/modulefiles
  module load pmi-shim
  module load flux

  WRAPPER="jsrun -a 1 -c ALL_CPUS -g ALL_GPUS -n $flux_nnodes --bind=none --smpiargs=\"-disable_gpu_hooks\""
  env PMIX_MCA_gds="^ds12,ds21" \
      FLUXION_QMANAGER_OPTIONS='queue-params=queue-depth=32' \
      FLUXION_RESOURCE_OPTIONS="load-allowlist=node,core,gpu prune-filters=ALL:core,ALL:gpu reserve-vtx-vec=2000000 policy=first" \
  $WRAPPER flux start -o,-S,log-filename=$flux_log,-Sstatedir=$flux_root -v $flux_bootstrap $flux_server &
  echo ""
elif [[ "$MACHINE" == *"frontier"* ]] ; then
  NBCORES=$(echo $SLURM_JOB_CPUS_PER_NODE | cut -d '(' -f1)
  echo "(`hostname`: `date`) --> Found ${NBCORES} cores and ${SLURM_GPUS_ON_NODE} GPU on node (${SLURM_JOB_NUM_NODES} nodes in allocation)"

  WRAPPER="srun -N ${flux_nnodes} --ntasks-per-node=1 --cpus-per-task=${NBCORES} --gpus-per-task=${SLURM_GPUS_ON_NODE} --mpi=pmi2"
  env FLUXION_QMANAGER_OPTIONS='queue-params=queue-depth=32' \
      FLUXION_RESOURCE_OPTIONS="load-allowlist=node,core,gpu prune-filters=ALL:core,ALL:gpu reserve-vtx-vec=2000000 policy=first" \
  $WRAPPER flux start -o,-S,log-filename=$flux_log,-Sstatedir=$flux_root -v $flux_bootstrap $flux_server &
echo ""
else
  echo "(`hostname`: `date`) --> Error: $MACHINE not supported"
  exit 1
fi

# ------------------------------------------------------------------------------
# now, wait for the flux info file
$MUMMI_APP/setup/utils/wait_for_file.sh $flux_server
if [ ! -f $flux_info ]; then
  popd > /dev/null 2>&1
  exit 1
fi

# ------------------------------------------------------------------------------
# we have success
echo "(`hostname`: `date`) --> Flux launch successful"

# write an env file for flux that clients can load
if true; then
  echo "(`hostname`: `date`) --> Writing flux env to ($flux_client)"
  echo "# Flux launch env"                              > $flux_client
  echo "#  host = `hostname`"                          >> $flux_client
  echo "#  user = `whoami`"                            >> $flux_client
  echo "#  time = `date`"                              >> $flux_client 
  echo "#  log  = $flux_log"                           >> $flux_client
  echo ""                                              >> $flux_client
  echo "# env vars needed to access flux"              >> $flux_client
  echo "export FLUX_SSH=ssh"                           >> $flux_client
  echo "export FLUX_URI=`cat $flux_server`"            >> $flux_client
  echo ""                                              >> $flux_client
  echo "# alias for flux commands"                     >> $flux_client
  echo "alias flxls='flux jobs -a --count=1000000'"    >> $flux_client
  echo "alias flxat='flux job attach'"                 >> $flux_client
  echo "alias flxcan='flux job cancel'"                >> $flux_client
  echo "alias flxkil='flux job kill'"                  >> $flux_client
fi

# now, start accessing this flux instance
source $flux_client

echo "Flux version:"
flux version
FLUX_VERSION=$(version $(flux version | awk '/^commands/ {print $2}'))

# echo "   flux modules:"
# flux module list
echo ""
echo "   FLUX_URI    :" $FLUX_URI
echo "   flux_nodes  :" $(flux getattr size)
echo "   flux_log    :" $flux_log

if true; then
  echo "(`hostname`: `date`) --> Adjusting Flux's grace period to 2m..."
  flux module reload job-exec kill-timeout=15.0m
  sleep 10s

  MIN_VER_FLUX=0.57 # below that version we cannot reload jobspec buffer
  MIN_VER_FLUX_LONG=$(version ${MIN_VER_FLUX})
  # We need to remove leading 0 because they are interpreted as octal numbers in bash
  if [[ "${FLUX_VERSION#00}" -ge "${MIN_VER_FLUX_LONG#00}" ]]; then
    ## Increase the jobspec buffer (require flux-core >=0.57)
    echo "(`hostname`: `date`) --> Increasing Flux's jobsepc ingest buffer from 10M to 50M"
    flux module reload job-ingest buffer-size=50M
  fi
  sleep 10s
  # Deactivate the jobspec validator
  echo "(`hostname`: `date`) --> Deactivating Flux's jobsepc validator"
  flux module reload job-ingest disable-validator
  sleep 10s
fi
echo "(`hostname`: `date`) --> $(flux resource info)"

unset PMI_LIBRARY
popd >/dev/null 2>&1

# ------------------------------------------------------------------------------
