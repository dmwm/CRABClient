from CRABClient.Commands.SubCommand import SubCommand
from CRABClient import __version__
import CRABClient.Emulator

class request_type(SubCommand):
    """ Return the string of the workflow type identified by -t/--task option
    """
    visible = False

    def __call__(self):


        proxyfile = self.options.proxyfile if self.options.proxyfile else self.proxyfilename
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, proxyfile, proxyfile, version=__version__)

        self.logger.debug('Looking type for task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri, data = {'workflow': self.cachedinfo['RequestName'], 'subresource': 'type'})
        self.logger.debug('Task type %s' % dictresult['result'][0])
        return dictresult['result'][0]

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "--proxyfile",
                                 dest = "proxyfile",
                                 default = None,
                                 help = "Proxy file to use." )
