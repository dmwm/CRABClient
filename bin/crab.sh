#!/usr/bin/env bash
if [ -z "$CRAB_SOURCE_SCRIPT" ]; then
    CRAB_SOURCE_SCRIPT="/cvmfs/cms.cern.ch/crab3/crab.sh"
fi

## Determine if this is a submit command.
SUBMIT=false;
for i in "$@"; do
    if [ "$i" = "submit" ] || [ "$i" = "sub" ]; then
        SUBMIT=true;
    fi
done

## If it is a submit command run the bootstrap script
if [ "$SUBMIT" = true ]; then
    ## Get the bin directory so that we know where the crab3bootstrap is located. The directory will be added to the PATH
    CRAB3_BIN_ROOT=$(sh -c "source $CRAB_SOURCE_SCRIPT >/dev/null 2>/dev/null; if [ \$? -eq 0 ] && [ -d \$CRABCLIENT_ROOT ]; \
                        then echo \$CRABCLIENT_ROOT/bin; else exit 1; fi")
    if [ $? -ne 0 ]; then
        echo -e "Error while loading CRAB3 environment. Cannot find CRAB3 root directory inside:\n\t$CRAB_SOURCE_SCRIPT";
        exit 1;
    fi

    ## Get the location of the python packages of the CRAB3 client. We'll add them to the PYTHONPATH before executing the
    ## bootstrap script. We will only add the python packages, without setting the COMP environment)
    CRAB3_PY_ROOT=$(sh -c "source $CRAB_SOURCE_SCRIPT >/dev/null 2>/dev/null; if [ \$? -eq 0 ] && [ -d \$CRABCLIENT_ROOT ]; \
                        then echo \$CRABCLIENT_ROOT/\$PYTHON_LIB_SITE_PACKAGES; else exit 1; fi")
    if [ $? -ne 0 ]; then
        echo -e "Error while loading CRAB3 environment. Cannot find CRAB3 root directory inside:\n\t$CRAB_SOURCE_SCRIPT";
        exit 1;
    fi

    ## Set two temporary variable for PATH and PYTHONPATH. They are going to be expanded BEFORE running the following sh command
    export PYTHONPATH_TMP=$PYTHONPATH:$CRAB3_PY_ROOT
    export PATH_TMP=$PATH:$CRAB3_BIN_ROOT
    PARAMS=$@
    BOOTSTRAP_OUT=$(sh -c "export PATH=$PATH_TMP:$PATH; export PYTHONPATH=$PYTHONPATH_TMP:$PYTHONPATH; crab3bootstrap $PARAMS; \
                                    if [ \$? -ne 0 ]; then exit 1; fi")
    #if the script does not exit with 0 then the output is the error message.
    if [ $? -ne 0 ]; then
        echo "$BOOTSTRAP_OUT"
        exit 1;
    fi
    ## The submit command will know what to do if this variable is set! See TODO: add link to an hypothetical twiki
    export CRAB3_BOOTSTRAP_DIR=$BOOTSTRAP_OUT
fi

## Execute the client command in a pure COMP environment
eval `scram unset -sh`
source $CRAB_SOURCE_SCRIPT

crab "$@"
## Remove the temporary direcotry, but only if we run the bootstrap script
[ -z "$CRAB3_BOOTSTRAP_DIR" ] || rm -rf "$CRAB3_BOOTSTRAP_DIR"
