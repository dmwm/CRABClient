# silence pylint complaints about things we need for Python 2.6 compatibility
# pylint: disable=unspecified-encoding, raise-missing-from, consider-using-f-string

from optparse import OptionParser  # pylint: disable=deprecated-module

from ServerUtilities import SERVICE_INSTANCES
from CRABClient import __version__ as client_version


class CRABOptParser(OptionParser):
    """
    Allows to make OptionParser behave how we prefer
    """

    def __init__(self, subCommands=None):
        """ Initialize the option parser used in the the client. That's only the first step parsing
            which basically creates the help and looks for the --debug/--quiet options. Each command
            than has its own set of arguments (some are shared, see CRABCmdOptParser ).

            subCommands: if present used to prepare a nice help summary for all the commands
        """
        usage  = "usage: %prog [options] COMMAND [command-options] [args]"
        epilog = ""
        if subCommands:
            epilog = '\nValid commands are: \n'
            for k in sorted(subCommands.keys()):
                epilog += '  %s' % subCommands[k].name
                epilog += ''.join( [' (%s)' % name for name in subCommands[k].shortnames ] )
                epilog += '\n'
            epilog += "To get single command help run:\n  crab command --help|-h\n"

            epilog += '\nFor more information on how to run CRAB-3 please follow this link:\n'
            epilog += 'https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookCRAB3Tutorial\n'

        OptionParser.__init__(self, usage   = usage, epilog  = epilog,
                                version = "CRAB client %s" % client_version
                             )

        # This is the important bit
        self.disable_interspersed_args()

        self.add_option( "--quiet",
                                action = "store_true",
                                dest = "quiet",
                                default = False,
                                help = "don't print any messages to stdout" )

        self.add_option( "--debug",
                                action = "store_true",
                                dest = "debug",
                                default = False,
                                help = "print extra messages to stdout" )


    def format_epilog(self, formatter):
        """
        do not strip the new lines from the epilog
        """
        return self.epilog



class CRABCmdOptParser(OptionParser):
    """ A class that extract the pieces for parsing the command line arguments
        of the CRAB commands.

    """

    def __init__(self, cmdname, doc, disable_interspersed_args):
        """
            doc:        the description of the command. Taken from self.__doc__
            disable_interspersed_args: some commands (e.g.: submit) allow to overwrite configuration parameters
        """
        usage = "usage: %prog " + cmdname + " [options] [args]"
        OptionParser.__init__(self, description = doc, usage = usage, add_help_option = True)
        if disable_interspersed_args:
            self.disable_interspersed_args()


    def addCommonOptions(self, cmdconf):
        """
            cmdconf:    the command configuration from the ClientMapping
            Note that default has to be None for most options in order not to
            override what is in the crab configuration file in case the option is
            not specified in the command line. E.g. the default instance in case
            a value is not indicated anywhere by the user is defined in ClientMapping.py
        """
        if cmdconf['requiresDirOption']:
            self.add_option("-d", "--dir",
                                   dest = "projdir",
                                   default = None,
                                   help = "Path to the CRAB project directory for which the crab command should be executed.")
            self.add_option("--task",
                                dest = "cmptask",
                                default = None,
                                help = "In alternative to -d, a complete task name. Can be taken from 'crab status' output, or from dashboard.")

        if cmdconf['requiresREST']:
            self.add_option("--instance",
                                   dest = "instance",
                                   type = "string",
                                   default = None,
                                   help = "Running instance of CRAB service." \
                                          " Needed whenever --task is used." \
                                          " Default value is 'prod'. " \
                                          " Valid values are %s." \
                                          % str(list(SERVICE_INSTANCES.keys())) )

        if cmdconf['requiresProxyVOOptions']:
            self.add_option("--voRole",
                                   dest = "voRole",
                                   default = None)
            self.add_option("--voGroup",
                                   dest = "voGroup",
                                   default = None)

        self.add_option("--proxy",
                        dest="proxy",
                        default=False,
                        help="Use the given proxy. Skip Grid proxy creation and myproxy delegation.")
