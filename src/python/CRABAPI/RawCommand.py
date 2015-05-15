"""
    CRABAPI.RawCommand - wrapper if one wants to simply execute a CRAB command
        but doesn't want to subprocess.Popen()
"""
import CRABAPI
import logging
from CRABClient.ClientUtilities import getLoggers


# NOTE: Not included in unittests
class crabCommand:

    def __init__(self, command, *args, **kwargs):
        """ crabComand - executes a given command with certain arguments and returns
                         the raw result back from the client. Arguments are...
        """
        self.command = command
        #Converting all arguments to a list. Adding '--' and '='
        self.arguments = []
        for key, val in kwargs.iteritems():
            self.arguments.append('--'+str(key))
            self.arguments.append(val)
        self.arguments.extend(list(args))
        self.logLevel = logging.INFO

    def __call__(self):
        return execRaw(self.command, self.arguments, self.logLevel)

    def quiet(self):
        self.logLevel = 60

    def setLogLevel(self, logLevel):
        self.logLevel = logLevel

# NOTE: Not included in unittests
def execRaw(command, args, logLevel = logging.INFO):
    """
        execRaw - executes a given command with certain arguments and returns
                  the raw result back from the client. args is a python list,
                  the same python list parsed by the optparse module
    """
    logger, _ = getLoggers(logLevel)
    try:
        mod = __import__('CRABClient.Commands.%s' % command, fromlist=command)
    except ImportError:
        raise CRABAPI.BadArgumentException( \
                                        'Could not find command "%s"' % command)

    try:
        cmdobj = getattr(mod, command)(logger, args)
        res = cmdobj()
    except SystemExit, se:
        # most likely an error from the OptionParser in Subcommand.
        # CRABClient #4283 should make this less ugly
        if se.code == 2:
            raise CRABAPI.BadArgumentException
    return res
