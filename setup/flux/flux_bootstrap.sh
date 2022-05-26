#!/usr/bin/env bash
# ------------------------------------------------------------------------------

flux_info=$1
echo -n "(`hostname`: `date`) --> Bootstrapping flux: "
echo "ssh://$(hostname)$(flux getattr local-uri | sed -e 's!local://!!')" | tee $flux_info
#flux resource drain 0
sleep inf

# ------------------------------------------------------------------------------
