#!/usr/bin/env bash
# ------------------------------------------------------------------------------

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

echo "   flux        = `which flux`"
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
# --smpiargs="-disable_gpu_hooks"
unset OMP_NUM_THREADS
# from open_source branch tested on lassen
#env PMIX_MCA_gds="^ds12,ds21"
# campaign4 code from summit

echo ""
env \
 FLUXION_QMANAGER_OPTIONS='queue-params=queue-depth=32' \
 FLUXION_RESOURCE_OPTIONS="load-allowlist=node,core,gpu prune-filters=ALL:core,ALL:gpu reserve-vtx-vec=2000000 policy=first" \
 jsrun -a 1 -c ALL_CPUS -g ALL_GPUS --bind=none -n $flux_nnodes \
 flux start -o,-S,log-filename=$flux_log -v $flux_bootstrap $flux_server &

# ------------------------------------------------------------------------------
# Saved version of above line from old repo:
#PMI_LIBRARY=$PMI jsrun -a 1 -c ALL_CPUS -g ALL_GPUS --bind=none -n $1 $SCRIPTDIR/env_clean.sh $FLUX start -o,-S,log-filename=flux.log,--setattr=persist-directory=$FLUX_ROOT $SCRIPTDIR/ip.sh

# ------------------------------------------------------------------------------
# now, wait for the flux info file
$MUMMI_APP/setup/utils/wait_for_file.sh $flux_server
if [ ! -f $flux_info ]; then
  popd >/dev/null 2>&1
  exit 1
fi

# ------------------------------------------------------------------------------
# we have success
echo "(`hostname`: `date`) --> Flux launch successful"

# write an env file for flux that clients can load
if true; then
 echo "(`hostname`: `date`) --> Writing flux env to ($flux_client)"
 echo "# Flux launch env"                   > $flux_client
 echo "#  host = `hostname`"               >> $flux_client
 echo "#  user = `whoami`"                 >> $flux_client
 echo "#  time = `date`"                   >> $flux_client 
 echo "#  log  = $flux_log"                >> $flux_client
 echo ""                                   >> $flux_client
 echo "# env vars needed to access flux"   >> $flux_client
 echo "export FLUX_SSH=ssh"                >> $flux_client
 echo "export FLUX_URI=`cat $flux_server`" >> $flux_client
 echo ""                                   >> $flux_client
 echo "# alias for flux commands"          >> $flux_client
 echo "alias flxls='flux jobs -a'"         >> $flux_client
 echo "alias flxat='flux job attach'"      >> $flux_client
 echo "alias flxcan='flux job cancel'"     >> $flux_client
 echo "alias flxkil='flux job kill'"       >> $flux_client
fi 

# now, start accessing this flux instance
source $flux_client

#echo "   flux version:"
#flux version
#echo "   flux modules:"
#flux module list
#echo "   FLUX_URI    :" $FLUX_URI
#echo "   flux_nodes  :" `flux getattr size`
#echo "   flux_log    :" $flux_log

if true; then
  echo "(`hostname`: `date`) --> Adjusting Flux's grace period to 2m..."
  flux module reload job-exec kill-timeout=15.0m
  sleep 10s
fi

unset PMI_LIBRARY
popd >/dev/null 2>&1

# ------------------------------------------------------------------------------
