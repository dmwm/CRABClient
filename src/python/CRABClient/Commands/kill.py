from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import RESTCommunicationException
import urllib

class kill(SubCommand):
    """
    Simply call the server side of the kill
    """
    visible = True

    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename)

        self.logger.debug('Killing task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.delete(self.uri, data = urllib.urlencode({ 'workflow' : self.cachedinfo['RequestName']}))
        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem killing task %s:\ninput:%s\noutput:%s\nreason:%s" % \
                    (self.cachedinfo['RequestName'], str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        self.logger.info("Task killed")
