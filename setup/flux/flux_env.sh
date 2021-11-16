#!/usr/bin/env bash
# ------------------------------------------------------------------------------

if [ ! -z $FLUX_MODULE_FILE ];
then
   echo '> Loading flux environment ('$MUMMI_HOST')'
   module use $FLUX_MODULE_FILE
   module load $FLUX_SHIM_MODULE

   if [ ! -z $FLUX_MPI_MODULE ];
   then
      module load $FLUX_MPI_MODULE
   fi
fi

export FLUX_SSH="ssh"

# add some alias commands to handle flux easily
#echo "> Setting up Flux aliases"
alias flxuri='export FLUX_URI=`cat $MUMMI_ROOT/flux/flux.info`'
alias flxls='flux jobs -a'
alias flxat='flux job attach'
alias flxcan='flux job cancel'

#echo "> Loading Flux environment complete."

# ------------------------------------------------------------------------------
