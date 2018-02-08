#!/usr/bin/env bash
if [ -z "$CRAB_SOURCE_SCRIPT" ]; then
    CRAB_SOURCE_SCRIPT="/cvmfs/cms.cern.ch/crab3/crab_standalone.sh"
fi

function getVariableValue {
    VARNAME=$1
    SUBDIR=$2
    sh -c "source $CRAB_SOURCE_SCRIPT >/dev/null 2>/dev/null; if [ \$? -eq 0 ] && [ -d $VARNAME ]; \
                    then echo $VARNAME/$SUBDIR; else exit 1; fi"
}

CRAB3_BIN_ROOT=$(getVariableValue \$CRABCLIENT_ROOT bin)
CRAB3_ETC_ROOT=$(getVariableValue \$CRABCLIENT_ROOT etc)
CRAB3_PY_ROOT=$(getVariableValue \$CRABCLIENT_ROOT \$PYTHON_LIB_SITE_PACKAGES)
DBS3_PY_ROOT=$(getVariableValue \$DBS3_CLIENT_ROOT \$PYTHON_LIB_SITE_PACKAGES)
DBS3_PYCURL_ROOT=$(getVariableValue \$DBS3_PYCURL_CLIENT_ROOT \$PYTHON_LIB_SITE_PACKAGES)
DBS3_CLIENT_ROOT=$(getVariableValue \$DBS3_CLIENT_ROOT)

if [ $# -gt 0 ] && [ "$1" == "-csh" ]; then
    echo "setenv PYTHONPATH $CRAB3_PY_ROOT:$DBS3_PY_ROOT:$DBS3_PYCURL_ROOT:$PYTHONPATH; \
                   setenv PATH $CRAB3_BIN_ROOT:$PATH; setenv DBS3_CLIENT_ROOT $DBS3_CLIENT_ROOT"
else
    export PYTHONPATH=$CRAB3_PY_ROOT:$DBS3_PY_ROOT:$DBS3_PYCURL_ROOT:$PYTHONPATH
    export PATH=$CRAB3_BIN_ROOT:$PATH
    export DBS3_CLIENT_ROOT
    if [ -n "$BASH" ]; then
        source $CRAB3_ETC_ROOT/crab-bash-completion.sh
    fi
fi
