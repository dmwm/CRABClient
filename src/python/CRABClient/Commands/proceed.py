from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import RESTCommunicationException
from CRABClient import __version__
import CRABClient.Emulator

import urllib

class proceed(SubCommand):
    """
    Continue submission of a task which was initialized with 'crab submit --dryrun'
    """

    def __init__(self, logger, cmdargs=None):
        """
        Class constructor.
        """
        SubCommand.__init__(self, logger, cmdargs)

    def __call__(self):
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version = __version__)

        msg = "Continuing submission of task %s" % (self.cachedinfo['RequestName'])
        self.logger.debug(msg)

        configreq = {'workflow': self.cachedinfo['RequestName'], 'subresource': 'proceed'}

        self.logger.info("Sending the request to the server")
        self.logger.debug("Submitting %s " % str(configreq))
        dictresult, status, reason = server.post(self.uri, data=urllib.urlencode(configreq))
        self.logger.debug("Result: %s" % (dictresult))
        if status != 200:
            msg = "Problem continuing task submission:\ninput:%s\noutput:%s\nreason:%s" \
                  % (str(data), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)
        msg = "Task continuation request successfuly sent to the CRAB3 server."
        if dictresult['result'][0]['result'] != 'ok':
            msg += "\nServer responded with: '%s'" % (dictresult['result'][0]['result'])
            returndict = {'status': 'FAILED'}
        else:
            returndict = {'status': 'SUCCESS'}
        self.logger.info(msg)

        return returndict
