# silence pylint complaints about things we need for Python 2.6 compatibility
# pylint: disable=unspecified-encoding, raise-missing-from, consider-using-f-string

import sys
from argparse import ArgumentParser

from ServerUtilities import SERVICE_INSTANCES
from CRABClient import __version__ as client_version

def _split_at_first_positional(parser, argv):
    """
    Emulate optparse.disable_interspersed_args:
    parse options only until the first positional token, consuming option values.
    """
    idx = 0
    size = len(argv)
    while idx < size:
        tok = argv[idx]
        if tok == "--":
            idx += 1
            break
        if tok == "-" or not tok.startswith("-"):
            break

        optname = tok.split("=", 1)[0] if tok.startswith("--") else tok
        action = parser._option_string_actions.get(optname)  # pylint: disable=protected-access
        idx += 1
        if action is None:
            continue
        if tok.startswith("--") and "=" in tok:
            continue

        nargs = action.nargs
        if nargs in (None, 1):
            if idx < size:
                idx += 1
        elif nargs in (0,):
            continue
        elif nargs == "?":
            if idx < size and not argv[idx].startswith("-"):
                idx += 1
        elif isinstance(nargs, int):
            idx += nargs
        else:
            idx = size

    return argv[:idx], argv[idx:]

class CRABArgParser(ArgumentParser):
    def __init__(self, *args, disable_interspersed_args=False, **kwargs):
        super().__init__(*args, **kwargs)
        self._disable_interspersed_args = disable_interspersed_args

    def parse_cmd(self, argv=None):
        argv = list(sys.argv[1:] if argv is None else argv)

        if not self._disable_interspersed_args:
            if "--" in argv:
                marker = argv.index("--")
                parseable = argv[:marker]
                tail = argv[marker + 1:]
            else:
                parseable = argv
                tail = []

            ns, rest = self.parse_known_args(parseable)
            unknown = [arg for arg in rest if arg.startswith("-") and arg != "-"]
            if unknown:
                self.error("no such option: %s" % unknown[0])
            return ns, rest + tail

        opt_argv, rest = _split_at_first_positional(self, argv)
        ns = self.parse_args(opt_argv)
        return ns, rest


class CRABOptParser(CRABArgParser):
    """
    Allows to make OptionParser behave how we prefer
    """

    def __init__(self, subCommands=None):
        """ Initialize the option parser used in the the client. That's only the first step parsing
            which basically creates the help and looks for the --debug/--quiet options. Each command
            than has its own set of arguments (some are shared, see CRABCmdOptParser ).

            subCommands: if present used to prepare a nice help summary for all the commands
        """
        usage = "usage: %(prog)s [options] COMMAND [command-options] [args]"
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

        CRABArgParser.__init__(self, usage=usage, epilog=epilog, disable_interspersed_args=True)

        self.add_argument("--version", action="version", version="CRAB client %s" % client_version)
        self.add_argument( "--quiet",
                                action = "store_true",
                                dest = "quiet",
                                default = False,
                                help = "don't print any messages to stdout" )

        self.add_argument( "--debug",
                                action = "store_true",
                                dest = "debug",
                                default = False,
                                help = "print extra messages to stdout" )


class CRABCmdOptParser(CRABArgParser):
    """ A class that extract the pieces for parsing the command line arguments
        of the CRAB commands.

    """

    def __init__(self, cmdname, doc, disable_interspersed_args=False):
        """
            doc:        the description of the command. Taken from self.__doc__
            disable_interspersed_args: some commands (e.g.: submit) allow to overwrite configuration parameters
        """
        usage = "usage: %(prog)s " + cmdname + " [options] [args]"
        CRABArgParser.__init__(
            self,
            description=doc,
            usage=usage,
            add_help=True,
            disable_interspersed_args=disable_interspersed_args
        )

    def addCommonOptions(self, cmdconf):
        """
            cmdconf:    the command configuration from the ClientMapping
            Note that default has to be None for most options in order not to
            override what is in the crab configuration file in case the option is
            not specified in the command line. E.g. the default instance in case
            a value is not indicated anywhere by the user is defined in ClientMapping.py
        """
        if cmdconf['requiresDirOption']:
            self.add_argument("-d", "--dir",
                              dest="projdir",
                              default=None,
                              help="Path to the CRAB project directory for which the crab command should be executed.")
            self.add_argument("--task",
                              dest="cmptask",
                              default=None,
                              help="In alternative to -d, a complete task name. Can be taken from 'crab status' output, or from dashboard.")

        if cmdconf['requiresREST']:
            self.add_argument("--instance",
                              dest="instance",
                              type=str,
                              default=None,
                              help="Running instance of CRAB service."
                                   " Needed whenever --task is used."
                                   " Default value is 'prod'. "
                                   " Valid values are %s."
                                   % str(list(SERVICE_INSTANCES.keys())))

        if cmdconf['requiresProxyVOOptions']:
            self.add_argument("--voRole",
                              dest="voRole",
                              default=None)
            self.add_argument("--voGroup",
                              dest="voGroup",
                              default=None)

        self.add_argument("--proxy",
                          dest="proxy",
                          default=False,
                          help="Use the given proxy. Skip Grid proxy creation and myproxy delegation.")
