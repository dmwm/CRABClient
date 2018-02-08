#!/usr/bin/env bash
if [ -z "$CRAB_SOURCE_SCRIPT" ]; then
    CRAB_SOURCE_SCRIPT="/cvmfs/cms.cern.ch/crab3/crab_pre_standalone.sh"
fi

init_light_source=${BASH_SOURCE}
readlink -q $init_light_source >/dev/null 2>&1 && init_light_source=$(readlink $init_light_source)
init_light_source=${init_light_source%%-pre.sh}.sh

source $init_light_source
