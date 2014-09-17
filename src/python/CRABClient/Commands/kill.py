from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_exceptions import RESTCommunicationException
from CRABClient.client_utilities import validateJobids
from CRABClient import __version__
import CRABClient.Emulator

from urllib import urlencode

class kill(SubCommand):
    """
    Command to kill submitted task, user must give a taskname to be kill
    """
    visible = True

    def __call__(self):
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.debug('Killing task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.delete(self.uri, data = urlencode({ 'workflow' : self.cachedinfo['RequestName']}) + '&' + urlencode(self.jobids))
        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem killing task %s:\ninput:%s\noutput:%s\nreason:%s" % \
                    (self.cachedinfo['RequestName'], str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        self.logger.info("Kill request successfully sent")
        if dictresult['result'][0]['result'] != 'ok':
            resultdict = {'status' : 'FAILED'}
            self.logger.info(dictresult['result'][0]['result'])
        else:
            resultdict = {'status' : 'SUCCESS'}

        return resultdict
    
    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '--jobids',
                                dest = 'jobids',
                                default = None,
                                help = 'Ids of the jobs you want to kill. Comma separated list of integers.',
                                metavar = 'JOBIDS' )

    def validateOptions(self):
        SubCommand.validateOptions(self)

        #check the format of jobids
        self.jobids = ''
        if getattr(self.options, 'jobids', None):
            self.jobids = validateJobids(self.options.jobids)
