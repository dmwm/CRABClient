"""
This is simply taking care of registering the user
"""

from Commands import CommandResult
import json
from Commands.SubCommand import SubCommand
from ServerInteractions import HTTPRequests
from client_utilities import validServerURL


class reg_user(SubCommand):
    """
    Register the user to the CRABServer
    """

    visible = False

    ## name should become automatically generated
    name  = "reg_user"
    usage = "usage: %prog " + name + " [options] [args]"


    def __call__(self):

        self.logger.debug("Registering user")

        server = HTTPRequests(self.options.server)

        userdefault = {
                       "Group"    : self.options.group,
                       "Team"     : self.options.team,
                       "Email"    : self.options.email,
                       "UserDN"   : self.options.dn
                      }

        self.logger.debug("Registering the user on the server")
        dictresult, status, reason = server.post( self.uri, json.dumps( userdefault, sort_keys = False) )
        self.logger.debug("Result: %s" % str(dictresult))
        if status != 200:
            msg = "Problem registering user:\ninput:%s\noutput:%s\nreason:%s" % (str(userdefault), str(dictresult), str(reason))
            return CommandResult(1, msg)

        return CommandResult(0, dictresult)


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """

        self.parser.add_option( "-g", "--group",
                                 dest = "group",
                                 default = "Analysis",
                                 help = "User group, default analysis" )

        self.parser.add_option( "-t", "--team",
                                 dest = "team",
                                 default = "Analysis",
                                 help = "User team, default Analysis" )

        self.parser.add_option( "-m", "--email",
                                 dest = "email",
                                 default = None,
                                 help = "User e-mail" )

        self.parser.add_option( "-c", "--certificate-subject",
                                 dest = "dn",
                                 default = None,
                                 help = "User group" )

        self.parser.add_option( "-s", "--server",
                                 dest = "server",
                                 default = None,
                                 action = "callback",
                                 type   = 'str',
                                 nargs  = 1,
                                 callback = validServerURL,
                                 metavar = "http://HOSTNAME:PORT",
                                 help = "Endpoint server url to use" )

