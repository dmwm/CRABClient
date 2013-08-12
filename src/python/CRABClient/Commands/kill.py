from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import RESTCommunicationException
from WMCore.Credential.Proxy import Proxy
from CRABClient.client_utilities import validateJobids

from urllib import urlencode


class kill(SubCommand):
    """
    Simply call the server side of the kill
    """
    visible = True

    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename)

        self.logger.debug('Killing task %s' % self.cachedinfo['RequestName'])

        if getattr(self.cachedinfo['OriginalConfig'].General, 'standalone', False):
            # Talk to HTCondor either directly or via gsissh
            # NOTE: have to import here to keep from circular imports
            import CRABInterface.DagmanDataWorkflow as DagmanModule
            dag = DagmanModule.DagmanDataWorkflow(
                                    config = self.cachedinfo['OriginalConfig'],
                                        )
            proxy = Proxy({'logger': self.logger})
            status = 200
            reason = 'OK'
            userdn = proxy.getSubject(self.proxyfilename)
            dictresult = dag.kill(self.cachedinfo['RequestName'], True, userdn=userdn)

        else:
            # Talk with a CRABServer
            dictresult, status, reason = server.delete(self.uri, data = urllib.urlencode({ 'workflow' : self.cachedinfo['RequestName']}))

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem killing task %s:\ninput:%s\noutput:%s\nreason:%s" % \
                    (self.cachedinfo['RequestName'], str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        self.logger.info("Kill request succesfully sent")
        if dictresult['result'][0]['result'] != 'ok':
            self.logger.info(dictresult['result'][0]['result'])


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '-i', '--jobids',
                                dest = 'jobids',
                                default = None,
                                help = 'Ids of the jobs you want to kill. Comma separated list of intgers',
                                metavar = 'JOBIDS' )

    def validateOptions(self):
        SubCommand.validateOptions(self)

        #check the format of jobids
        self.jobids = ''
        if getattr(self.options, 'jobids', None):
            self.jobids = validateJobids(self.options.jobids)
