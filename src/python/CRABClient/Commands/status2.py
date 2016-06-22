from __future__ import division # I want floating points

import CRABClient.Emulator
from CRABClient import __version__

from CRABClient.UserUtilities import getFileFromURL
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import ConfigurationException

from ServerUtilities import getProxiedWebDir


class status2(SubCommand):
    """
    Query the status of your tasks, or detailed information of one or more tasks
    identified by the -d/--dir option.
    """

    shortnames = ['st2']

    def __call__(self):
        taskname = self.cachedinfo['RequestName']
        inputlist = {'subresource': 'webdir', 'workflow': taskname}
        uri = self.getUrl(self.instance, resource = 'task')
        webdir = getProxiedWebDir(taskname, self.serverurl, uri, self.proxyfilename, self.logger.debug)
        filename = 'status_cache' #TODO Emilis fill find the name
        if not webdir:
            serverFactory = CRABClient.Emulator.getEmulator('rest')
            server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)
            dictresult, _, _ =  server.get(uri, data = inputlist)
            webdir = dictresult['result'][0]
            self.logger.info('Server result: %s' % webdir)
        url = webdir + '/' + filename
        longStReport = getFileFromURL(url, proxyfilename=self.proxyfilename)
        self.logger.info(open(longStReport).read())


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """

        self.parser.add_option("--long",
                               dest = "long",
                               default = False,
                               action = "store_true",
                               help = "Print one status line per job.")
        self.parser.add_option("--sort",
                               dest = "sort",
                               default = None,
                               help = "Sort failed jobs by 'state', 'site', 'runtime', 'memory', 'cpu', 'retries', 'waste' or 'exitcode'.")
        self.parser.add_option("--json",
                               dest = "json",
                               default = False,
                               action = "store_true",
                               help = "Print status results in JSON format.")
        self.parser.add_option("--summary",
                               dest = "summary",
                               default = False,
                               action = "store_true",
                               help = "Print site summary.")
        self.parser.add_option("--idle",
                               dest = "idle",
                               default = False,
                               action = "store_true",
                               help = "Print summary for idle jobs.")
        self.parser.add_option("--verboseErrors",
                               dest = "verboseErrors",
                               default = False,
                               action = "store_true",
                               help = "Expand error summary, showing error messages for all failed jobs.")


    def validateOptions(self):
        SubCommand.validateOptions(self)
        if self.options.idle and (self.options.long or self.options.summary):
            raise ConfigurationException("Option --idle conflicts with --summary and --long")

