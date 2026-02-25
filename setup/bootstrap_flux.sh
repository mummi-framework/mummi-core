#!/usr/bin/env bash
# Copyright 2021-2023 Lawrence Livermore National Security, LLC and other
#
# SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

usage="Usage: $(basename "$0") [#NODES] -- Script that bootstrap Flux on NNODES."

function version() {
  echo "$@" | awk -F. '{ printf("%d%03d%03d%03d\n", $1,$2,$3,$4); }';
}

# Check if the allocation has the right size
# args:
#   - $1 : number of nodes requested for Flux
function check_main_allocation() {
  if [[ "$(flux getattr size)" -eq "$1" ]]; then
    echo "[$(date +'%m%d%Y-%T')@$(hostname)] Flux launch successful with $(flux getattr size) nodes"
  else
    echo "[$(date +'%m%d%Y-%T')@$(hostname)] Error: Requested nodes=$1 but Flux allocation size=$(flux getattr size)"
    exit 1
  fi
}

# Check if the 3 inputs are integers
function check_input_integers() {
  re='^[0-9]+$'
  if ! [[ $1 =~ $re && $2 =~ $re && $3 =~ $re ]] ; then
    echo "[$(date +'%m%d%Y-%T')@$(hostname)] Error: number of nodes is not an integer ($1, $2, $3)"
    exit 1
  fi
}

# Wait for a file to be created
#   - $1 : the file
#   - $2 : Max number of retry (one retry every 5 seconds)
function wait_for_file() {
  local FLUX_SERVER="$1"
  local EXIT_COUNTER=0
  local MAX_COUNTER="$2"
  while [ ! -f $FLUX_SERVER ]; do
    sleep 5s
    echo "[$(date +'%m%d%Y-%T')@$(hostname)] $FLUX_SERVER does not exist yet."
    exit_counter=$((EXIT_COUNTER + 1))
    if [ "$EXIT_COUNTER" -eq "$MAX_COUNTER" ]; then
      echo "[$(date +'%m%d%Y-%T')@$(hostname)] Timeout: Failed to find file (${FLUX_SERVER})."
      exit 1
    fi
  done
}

MACHINE=$(echo $(hostname --long) | sed -e 's/[0-9]*$//')

# ------------------------------------------------------------------------------
# spack packages
# ------------------------------------------------------------------------------
if [[ "$MACHINE" == *"lassen"* ]] ; then
  export MUMMI_SPACK_VER="0.21"
  export MUMMI_SPACK_ROOT="/usr/workspace/mummiusr/mummi-spack/"
  # Flux-core Minimum version required
  [[ -z ${MIN_VER_FLUX+z} ]] && MIN_VER_FLUX="0.45.0"
elif [[ "$MACHINE" == *"frontier"* ]] ; then
  export MUMMI_SPACK_VER="0.23"
  export MUMMI_SPACK_ROOT="/lustre/orion/lrn005/proj-shared/spack/mummi-dev-spack/"
  # Flux-core Minimum version required
  [[ -z ${MIN_VER_FLUX+z} ]] && MIN_VER_FLUX="0.45.0"
else
  echo "[$(date +'%m%d%Y-%T')@$(hostname)] machine $MACHINE is not supported at the moment."
  exit 1
fi

if [ ! -d "$MUMMI_SPACK_ROOT" ] ;then
  echo "[$(date +'%m%d%Y-%T')@$(hostname)] [$MUMMI_SPACK_ROOT] is not a valid Spack installation." >&2
  exit 1
fi

if [[ "$MACHINE" == *"lassen"* ]] ; then
  echo "[$(date +'%m%d%Y-%T')@$(hostname)] Loading LC managed installation"

  # spack sets these variable for Flux but they conflict with the installed Flux
  unset FLUX_CONNECTOR_PATH FLUX_EXEC_PATH FLUX_MODULE_PATH FLUX_RC_EXTRA LUA_PATH

  # SPACK_DIR="$MUMMI_SPACK_ROOT/spack/$MUMMI_SPACK_VER/share/spack/setup-env.sh"

  # To use module command we must source this file
  # Those options are needed on IBM machines (CORAL)
  # Documented: https://flux-framework.readthedocs.io/en/latest/tutorials/lab/coral.html
  source /etc/profile.d/z00_lmod.sh
  module use /usr/tce/modulefiles/Core
  module use /usr/global/tools/flux/blueos_3_ppc64le_ib/modulefiles
  module load pmi-shim
  module load flux
elif [[ "$MACHINE" == *"frontier"* ]] ; then
  echo "[$(date +'%m%d%Y-%T')@$(hostname)] Loading Spack installation ($MUMMI_SPACK_ROOT)"
  SPACK_DIR="$MUMMI_SPACK_ROOT/$MUMMI_SPACK_VER/spack/share/spack/setup-env.sh"

  source ${SPACK_DIR}
  echo "[$(date +'%m%d%Y-%T')@$(hostname)] Sourced ${SPACK_DIR}"

  export MUMMI_SPACK_TARGET=$(spack arch --target)
  echo "[$(date +'%m%d%Y-%T')@$(hostname)] Loading Spack target $MUMMI_SPACK_TARGET"
  spack load flux-sched target=$MUMMI_SPACK_TARGET
fi

# the script needs the number of nodes for flux
FLUX_NODES="$1"
TS="$2"

if [ -z "$TS" ]; then
  TS=$(date "+%Y%m%d-%H%M%S-%3N")
  echo "[$(date +'%m%d%Y-%T')@$(hostname)] No timestamp provided, creating a default one (${TS})"
fi

FLUX_PID="mummi-flux-pid-${TS}.log"
FLUX_SERVER="mummi-uri-${TS}.log"
FLUX_LOG="mummi-flux-${TS}.log"

re='^[0-9]+$'
if ! [[ $FLUX_NODES =~ $re ]] ; then
  echo "[$(date +'%m%d%Y-%T')@$(hostname)] ERROR: '$FLUX_NODES' is not a number."
  echo $usage  
  exit 1
fi

echo "[$(date +'%m%d%Y-%T')@$(hostname)] Launching Flux with $FLUX_NODES nodes"

unset FLUX_URI
export LC_ALL="C"
export FLUX_F58_FORCE_ASCII=1
export FLUX_SSH="ssh"
# Cleanup from previous runs
rm -f $FLUX_SERVER $FLUX_LOG $FLUX_PID

if ! [[ -x "$(command -v flux)" ]]; then
  echo "[$(date +'%m%d%Y-%T')@$(hostname)] Error: flux is not installed."
  exit 1
fi
echo "[$(date +'%m%d%Y-%T')@$(hostname)] flux = $(command -v flux)"

flux_version=$(version $(flux version | awk '/^commands/ {print $2}'))
MIN_VER_FLUX_LONG=$(version ${MIN_VER_FLUX})
# We need to remove leading 0 because they are interpreted as octal numbers in bash
if [[ "${flux_version#00}" -lt "${MIN_VER_FLUX_LONG#00}" ]]; then
  echo "[$(date +'%m%d%Y-%T')@$(hostname)] Error Flux $(flux version | awk '/^commands/ {print $2}') is not supported.\
  Bootstrap requires flux>=${MIN_VER_FLUX}"
  exit 1
fi

echo "[$(date +'%m%d%Y-%T')@$(hostname)] flux version"
flux version

# We create a Flux wrapper around sleep on the fly to get the main Flux URI
FLUX_SLEEP_WRAPPER="./$(mktemp flux-wrapper.XXXX.sh)"
cat << 'EOF' > $FLUX_SLEEP_WRAPPER
#!/usr/bin/env bash
echo "ssh://$(hostname)$(flux getattr local-uri | sed -e 's!local://!!')" > "$1"
echo "$$" > "$2"
sleep inf
EOF
chmod u+x $FLUX_SLEEP_WRAPPER

if [[ "$MACHINE" == *"lassen"* ]] ; then
  PMIX_MCA_gds="^ds12,ds21" \
    jsrun -a 1 -c ALL_CPUS -g ALL_GPUS -n ${FLUX_NODES} \
      --bind=none --smpiargs="-disable_gpu_hooks" \
      flux start -o,-S,log-filename=$FLUX_LOG -v $FLUX_SLEEP_WRAPPER $FLUX_SERVER $FLUX_PID &
elif [[ "$MACHINE" == *"frontier"* ]] ; then
  NBCORES=$(echo $SLURM_JOB_CPUS_PER_NODE | cut -d '(' -f1)
  echo "[$(date +'%m%d%Y-%T')@$(hostname)] Found ${NBCORES} cores and ${SLURM_GPUS_ON_NODE} GPU on node (${SLURM_JOB_NUM_NODES} nodes in allocation)"

  WRAPPER="srun -N ${FLUX_NODES} --ntasks-per-node=1 --cpus-per-task=${NBCORES} --gpus-per-task=${SLURM_GPUS_ON_NODE}"
  env FLUXION_QMANAGER_OPTIONS='queue-params=queue-depth=32' \
    FLUXION_RESOURCE_OPTIONS="load-allowlist=node,core,gpu prune-filters=ALL:core,ALL:gpu reserve-vtx-vec=2000000 policy=first" \
  $WRAPPER flux start -o,-S,log-filename=$FLUX_LOG -v $FLUX_SLEEP_WRAPPER $FLUX_SERVER $FLUX_PID &
fi

echo ""
# now, wait for the flux info file
# we retry 20 times (one retry every 5 seconds)
wait_for_file $FLUX_SERVER 20
export FLUX_URI=$(cat $FLUX_SERVER)
echo "[$(date +'%m%d%Y-%T')@$(hostname)] Run: export FLUX_URI=$(cat $FLUX_SERVER)"
echo "[$(date +'%m%d%Y-%T')@$(hostname)] To kill Flux: kill $(cat $FLUX_PID)"
rm -rf $FLUX_SLEEP_WRAPPER # we do need the wrapper anymore
check_main_allocation ${FLUX_NODES}


## Increase the jobspec buffer (require flux-core >=0.57)
# flux module reload job-ingest buffer-size=50M
## Deactivate the jobspec validator
# flux module reload job-ingest disable-validator
