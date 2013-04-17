from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import RESTCommunicationException
from WMCore.Credential.Proxy import Proxy
import urllib

class kill(SubCommand):
    """
    Simply call the server side of the kill
    """
    visible = True

    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename)

        self.logger.debug('Killing task %s' % self.cachedinfo['RequestName'])

        if not self.standalone:
            dictresult, status, reason = server.delete(self.uri, data = urllib.urlencode({ 'workflow' : self.cachedinfo['RequestName']}))
        else:
            proxy = Proxy({'logger': self.logger})
            dag = __import__("CRABInterface.DagmanDataWorkflow").DagmanDataWorkflow.DagmanDataWorkflow()
            status = 200
            reason = 'OK'
            userdn = proxy.getSubject(self.proxyfilename)
            dictresult = dag.kill(self.cachedinfo['RequestName'], True, userdn=userdn)
        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem killing task %s:\ninput:%s\noutput:%s\nreason:%s" % \
                    (self.cachedinfo['RequestName'], str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        self.logger.info("Task killed")
