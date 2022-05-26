#!/usr/bin/env bash
# ------------------------------------------------------------------------------

monitor_spec_file=$1

ts=`date "+%Y.%m.%d-%H.%M.%S"`
log_file=monitoring.${ts}.log

## temporary for summit 2022
spack load py-pip
spack unload openssl

mkdir -p $MUMMI_ROOT/monitoring
pushd $MUMMI_ROOT/monitoring >/dev/null 2>&1

echo "(`hostname`: `date`) --> Launching MuMMI Monitoring (${monitor_spec_file})"

#mummi_monitor $monitor_spec_file
python3 $MUMMI_CORE/mummi_core/scripts/monitor_mummi.py $monitor_spec_file >> $log_file 2>&1 

sleep inf
popd >/dev/null 2>&1

# ------------------------------------------------------------------------------
