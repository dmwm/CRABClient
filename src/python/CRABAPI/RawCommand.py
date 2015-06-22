"""
    CRABAPI.RawCommand - wrapper if one wants to simply execute a CRAB command
        but doesn't want to subprocess.Popen()
"""
import CRABAPI
import logging
from CRABClient.ClientUtilities import initLoggers


# NOTE: Not included in unittests
def crabCommand(command, *args, **kwargs):
    """ crabComand - executes a given command with certain arguments and returns
                     the raw result back from the client. Arguments are...
    """
    #Converting all arguments to a list. Adding '--' and '='
    arguments = []
    for key, val in kwargs.iteritems():
        arguments.append('--'+str(key))
        arguments.append(val)
    arguments.extend(list(args))

    return execRaw(command, arguments)


# NOTE: Not included in unittests
def execRaw(command, args):
    """
        execRaw - executes a given command with certain arguments and returns
                  the raw result back from the client. args is a python list,
                  the same python list parsed by the optparse module
    """
    logger, _ = initLoggers()

    try:
        mod = __import__('CRABClient.Commands.%s' % command, fromlist=command)
    except ImportError:
        raise CRABAPI.BadArgumentException( \
                                        'Could not find command "%s"' % command)

    try:
        cmdobj = getattr(mod, command)(logger, args)
        res = cmdobj()
    except SystemExit as se:
        # most likely an error from the OptionParser in Subcommand.
        # CRABClient #4283 should make this less ugly
        if se.code == 2:
            raise CRABAPI.BadArgumentException
    return res
