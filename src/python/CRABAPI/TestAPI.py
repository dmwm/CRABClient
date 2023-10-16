#!/usr/bin/env python3
"""
allow to test one CRAB command via the API
usage:
 ./TestCommand.py <same line as you would type to use crab CLI>
examples:
  TestAPI.py checkusername
  TestAPI.py checkwrite --site T2_CH_CERN
  TestAPI.py checkwrite --site=T2_CH_CERN
  TestAPI.py checkdataset --dataset=/HIForward18/HIRun2023A-v1/RAW
  etc.
"""

import sys
import CRABClient
import pprint
from CRABAPI.RawCommand import execRaw
from CRABClient.ClientExceptions import ClientException

def crab_cmd(command, arguments):

    try:
        output = crabCommand(command, arguments)
        return output
    except HTTPException as hte:
        print('Failed', command, ': %s' % (hte.headers))
    except ClientException as cle:
        print('Failed', command, ': %s' % (cle))

def main():

    if len(sys.argv) == 1:
        print('No command given')
        return
    command = sys.argv[1]
    print(f"testing:  {command}")
    if len(sys.argv) > 2:
        arguments = sys.argv[2:]
    else:
        arguments = None

    result = execRaw(command, arguments)

    print (f"command returned:\n{pprint.pformat(result)}")


if __name__ == '__main__':
    main()
