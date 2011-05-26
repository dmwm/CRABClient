from Commands import CommandResult
from Commands.SubCommand import SubCommand
from ServerInteractions import HTTPRequests


class server_info(SubCommand):
    """
    Get relevant information about the server
    """

    visible = False

    ## name should become automatically generated
    name  = "server_info"


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-s", "--server",
                                 dest = "server",
                                 default = None,
                                 help = "Endpoint server url to use" )


    def __call__(self, options):

        (options, args) = self.parser.parse_args(options)

        server = HTTPRequests(options.server)

        self.logger.debug('Looking up server information')
        dictresult, status, reason = server.get(self.uri)

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(userdefault), str(dictresult), str(reason))
            return CommandResult(1, msg)

        return CommandResult(0, dictresult)
