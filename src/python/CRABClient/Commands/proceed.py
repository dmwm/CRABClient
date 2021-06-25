from __future__ import division
from __future__ import print_function

import sys

if sys.version_info >= (3, 0):
    from urllib.parse import urlencode # pylint: disable=E0611
if sys.version_info < (3, 0):
    from urllib import urlencode

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import RESTCommunicationException

class proceed(SubCommand):
    """
    Continue submission of a task which was initialized with 'crab submit --dryrun'
    """

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)

    def __call__(self):
        server = self.crabserver

        msg = "Continuing submission of task %s" % (self.cachedinfo['RequestName'])
        self.logger.debug(msg)

        request = {'workflow': self.cachedinfo['RequestName'], 'subresource': 'proceed'}

        self.logger.info("Sending the request to the server")
        self.logger.debug("Submitting %s " % str(request))
        result, status, reason = server.post(api=self.defaultApi, data=urlencode(request))
        self.logger.debug("Result: %s" % (result))
        if status != 200:
            msg = "Problem continuing task submission:\ninput:%s\noutput:%s\nreason:%s" \
                  % (str(request), str(result), str(reason))
            raise RESTCommunicationException(msg)
        msg = "Task continuation request successfully sent to the CRAB3 server"
        if result['result'][0]['result'] != 'ok':
            msg += "\nServer responded with: '%s'" % (result['result'][0]['result'])
            status = {'status': 'FAILED'}
        else:
            status = {'status': 'SUCCESS'}
            self.logger.info("To check task progress, use 'crab status'")
        self.logger.info(msg)

        return status
