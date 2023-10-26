#!/bin/bash
# returns the two env. variables needed to make Rucio client work
# on the current OS
unset PYTHONPATH
eval `scram unsetenv -sh`
source /cvmfs/cms.cern.ch/rucio/setup-py3.sh > /dev/null
echo $RUCIO_HOME $PYTHONPATH
