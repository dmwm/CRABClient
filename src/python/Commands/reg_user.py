"""
This is simply taking care of registering the user
"""

from Commands import CommandResult
import json
import os
from Commands.SubCommand import SubCommand
from ServerInteractions import HTTPRequests


class reg_user(SubCommand):
    """
    Register the user to the CRABServer
    """

    visible = False

    ## name should become automatically generated
    name  = "reg_user"
    usage = "usage: %prog " + name + " [options] [args]"


    def __call__(self, args):

        (options, args) = self.parser.parse_args( args )

        self.logger.debug("Registering user")

        server = HTTPRequests(options.server)

        defaultconfigreq = {"RequestType" : "Analysis"}

        userdefault = {
                       "Group"    : options.group,
                       "Team"     : options.team,
                       "Email"    : options.email,
                       "UserDN"   : options.dn
                      }
        print userdefault

        self.logger.debug("Registering the user on the server")
        useruri = '/crabinterface/crab/user'
        dictresult, status, reason = server.post( useruri, json.dumps( userdefault, sort_keys = False) )
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
                                 help = "Endpoint server url to use" )

