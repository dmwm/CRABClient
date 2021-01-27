
import urllib
from base64 import b64encode

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import RESTCommunicationException

class kill(SubCommand):
    """
    Command to kill submitted tasks. The user must give the crab project
    directory for the task he/she wants to kill.
    """
    visible = True

    def __call__(self):
        server = self.RESTServer

        self.logger.debug("Killing task %s" % self.cachedinfo['RequestName'])
        inputs = {'workflow' : self.cachedinfo['RequestName']}
        if self.options.killwarning:
            inputs.update({'killwarning' : b64encode(self.options.killwarning)})

        dictresult, status, reason = server.delete(self.uri, data=urllib.urlencode(inputs))
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
        self.parser.add_option('--killwarning',
                               dest='killwarning',
                               default=None,
                               help='A warning message to be appended to the warnings list shown by "crab status"')

    def validateOptions(self):
        SubCommand.validateOptions(self)
