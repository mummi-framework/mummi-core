#!/usr/bin/env bash
# ------------------------------------------------------------------------------

FLUX_INFO=$1
echo "ssh://$(hostname)$(flux getattr local-uri | sed -e 's!local://!!')" | tee $FLUX_INFO
#flux resource drain 0
echo `hostname`
sleep inf

# ------------------------------------------------------------------------------
