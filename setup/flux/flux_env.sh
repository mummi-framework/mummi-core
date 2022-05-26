#!/usr/bin/env bash
# ------------------------------------------------------------------------------

if [ ! -z $MUMMI_FLUX_MODULE_FILE ]; then
   echo "(`hostname`: `date`) --> Loading flux environment ($MUMMI_HOST)"
   module use $MUMMI_FLUX_MODULE_FILE
   module load $MUMMI_FLUX_SHIM_MODULE  # >/dev/null 2>&1

   if [ ! -z $MUMMI_FLUX_MPI_MODULE ]; then
      if [[ ! -z $MUMMI_FLUX_MPI_MODULE_FILE ]]; then
          module use $MUMMI_FLUX_MPI_MODULE_FILE
      fi
      module load $MUMMI_FLUX_MPI_MODULE # >/dev/null 2>&1
   fi

   # need to load the normal mpi again
   module load $MUMMI_MPI_MODULE   # >/dev/null 2>&1
fi

export FLUX_SSH="ssh"

# ------------------------------------------------------------------------------
