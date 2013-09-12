from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_exceptions import RESTCommunicationException
from CRABClient.client_utilities import validateJobids
from CRABClient import __version__

from RESTInteractions import HTTPRequests

from urllib import urlencode

class kill(SubCommand):
    """
    Simply call the server side of the kill
    """
    visible = True

    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.debug('Killing task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.delete(self.uri, data = urlencode({ 'workflow' : self.cachedinfo['RequestName']}) + '&' + urlencode(self.jobids))
        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem killing task %s:\ninput:%s\noutput:%s\nreason:%s" % \
                    (self.cachedinfo['RequestName'], str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        self.logger.info("Kill request successfully sent")
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
