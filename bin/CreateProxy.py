#!/usr/bin/env python
"""
This is a convenience script to create a long-living credential in myproxy.cern.ch
which can be used by CRAB. While CRAB Client does this transparently for users,
in some special cases it may be desirable to do it outside CRAB Client, e.g. for
HammerCloud were the operator will only want to execute this every few months.
SUch a procedure is not well suited for general users since when they are 1 month
away from their certificate expiration date they will start getting hard to
interpreter messages and different result (shortened credentials)

Executing the script requires the CRAB Client Python environment like when using CRAB Client Python API:
 https://twiki.cern.ch/twiki/bin/view/CMSPublic/CMSCrabClient#Using_CRABClient_API
i.e.
source /cvmfs/cms.cern.ch/common/crab-setup.sh

In Hammer Cloud case, this script is invoked as:
 ./CreateProxy.py -r production -s cmsweb.cern.ch -u /crabserver/prod/info -d 100
"""

import sys
import os
import logging
from optparse import OptionParser, OptionGroup


from CRABClient.CredentialInteractions import CredentialInteractions

from RESTInteractions import HTTPRequests

RENEW_MYPROXY_THRESHOLD = 15


logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
#logging.basicConfig(filename='credential.log', level=logging.DEBUG)

def server_info(subresource, server, proxyfilename, baseurl):
    """
    Get relevant information about the CRAB REST server
    """
    server = HTTPRequests(url=server, localcert=proxyfilename, localkey=proxyfilename, version='HC')

    dictresult, status, reason = server.get(baseurl, {'subresource' : subresource})

    return dictresult['result'][0]


def handleProxy(server, baseurl ,voRole, voGroup, days):
    """
    """
    proxy = CredentialInteractions('', '', voRole, voGroup, logging)
    #proxy.myproxyDesiredValidity = days
    proxy.setMyProxyValidity(int(days)*24*60)

    logging.info( "Checking credentials" )

    proxy.proxyInfo = proxy.createNewVomsProxy( timeLeftThreshold = 720 )
    proxyfilename = proxy.proxyInfo['filename']

    #get the dn of the agents from the server
    alldns = server_info('delegatedn', server, proxyfilename, baseurl)
    for serverdn in alldns['services']:
        proxy.defaultDelegation['serverDN'] = serverdn
        proxy.defaultDelegation['myProxySvr'] = 'myproxy.cern.ch'

        logging.info("Registering user credentials for server %s" % serverdn)
        proxy.createNewMyProxy( timeleftthreshold = 60 * 60 * 24 * RENEW_MYPROXY_THRESHOLD, nokey=True)
        proxy.createNewMyProxy2( timeleftthreshold = 60 * 60 * 24 * RENEW_MYPROXY_THRESHOLD, nokey=True)


if __name__ == '__main__' :

    baseurl = '/crabserver/preprod/info'
    basesrv = 'cmsweb-testbed.cern.ch'
    validity = 30

    parser = OptionParser()

    parser.add_option( "-g", "--group",
                        default = '')
    parser.add_option( "-r", "--role",
                        default = '')
    parser.add_option( "-s", "--server",
                        default = basesrv)
    parser.add_option( "-u", "--url",
                        default = baseurl)
    parser.add_option( "-d", "--days",
                        default = validity)

    (options, args) = parser.parse_args()

    handleProxy( options.server, options.url, options.role, options.group, options.days )

