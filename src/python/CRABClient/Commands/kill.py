from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import RESTCommunicationException
from CRABClient.ClientUtilities import validateJobids
from CRABClient import __version__
import CRABClient.Emulator

from urllib import urlencode
from base64 import b64encode


class kill(SubCommand):
    """
    Command to kill submitted tasks. The user must give the crab project
    directory for the task he/she wants to kill.
    """
    visible = True

    def __call__(self):
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.debug('Killing task %s' % self.cachedinfo['RequestName'])
        inputs = {'workflow' : self.cachedinfo['RequestName']}
        if self.options.killwarning:
            inputs.update({'killwarning' : b64encode(self.options.killwarning)})

        dictresult, status, reason = server.delete(self.uri, data = urlencode(inputs) + '&' + urlencode(self.jobids))
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

        self.parser.add_option( '--killwarning',
                                dest = 'killwarning',
                                default = None,
                                help = 'A warning message to be appended to the list of warnings shown by the "crab status"')

    def validateOptions(self):
        SubCommand.validateOptions(self)

        #check the format of jobids
        self.jobids = ''
        if getattr(self.options, 'jobids', None):
            self.jobids = validateJobids(self.options.jobids)
