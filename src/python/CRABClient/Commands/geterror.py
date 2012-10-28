from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_utilities import colors
from CRABClient.client_exceptions import MissingOptionException, RESTCommunicationException

class geterror(SubCommand):
    """
    Return the errors of the task
    identified by -t/--task option
    """

    shortnames = ['err']

    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename)

        self.logger.debug('Getting errors of task %s' % self.cachedinfo['RequestName'])
        options = { 'workflow' : self.cachedinfo['RequestName'], 'subresource' : 'fwjr' }
        if getattr(self.options, 'quantity', None):
            options.update({'limit' : self.options.quantity})
        if getattr(self.options, 'exitcode', None):
            options.update({'exitcode' : self.options.exitcode})
        dictresult, status, reason = server.get(self.uri, data = options)
        self.logger.debug(dictresult)

        if status != 200:
            msg = "Problem retrieving errors:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        def printerr(step, error):
            """Takes care of priting out to stdout"""
            if error and step != 'exitcode':
                self.logger.info("\t%sErrors for step: %s%s" % (colors.RED, colors.NORMAL, step))
                for err in error:
                    self.logger.info("\t\t%sError type: %s%s" % (colors.RED, colors.NORMAL, err['type']))
                    self.logger.info("\t\t%sMessage: %s%s" % (colors.RED, colors.NORMAL, err['details']))

        for fwjr in sorted(dictresult['result'], key=lambda x: x['exitcode']):
            self.logger.info("\n%sErrors for exit code %s%s" % (colors.RED, colors.NORMAL, fwjr['exitcode']))
            if 'PerformanceError' in fwjr:
                printerr('PerformanceError', fwjr['PerformanceError'])
            else:
                for step in fwjr:
                    printerr(step, fwjr[step])

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '-q', '--quantity',
                                dest = 'quantity',
                                help = 'A number which express the number of errors you want to retrieve (or all). Defaut one error per exitcode' )
        self.parser.add_option( '-e', '--exitcode',
                                dest = 'exitcode',
                                help = 'Retrieve the logs only for this exitcode' )

