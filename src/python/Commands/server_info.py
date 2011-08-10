from Commands import CommandResult
from Commands.SubCommand import SubCommand
from ServerInteractions import HTTPRequests
from client_utilities import validServerURL


class server_info(SubCommand):
    """
    Get relevant information about the server
    """

    visible = False
    name  = __name__.split('.').pop()
    names = [name]


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-s", "--server",
                                 dest = "server",
                                 default = None,
                                 action = "callback",
                                 type   = 'str',
                                 nargs  = 1,
                                 callback = validServerURL,
                                 metavar = "http://HOSTNAME:PORT",
                                 help = "Endpoint server url to use" )


    def __call__(self):

        server = HTTPRequests(self.options.server)

        self.logger.debug('Looking up server information')
        dictresult, status, reason = server.get(self.uri)

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving status:\n:output:%s\nreason:%s" % (str(dictresult), str(reason))
            return CommandResult(1, msg)

        return CommandResult(0, dictresult)
