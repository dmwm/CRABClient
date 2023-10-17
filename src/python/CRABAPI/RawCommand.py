"""
    CRABAPI.RawCommand - wrapper if one wants to simply execute a CRAB command
        but doesn't want to subprocess.Popen()
"""
import CRABAPI

import traceback

from CRABClient.ClientUtilities import initLoggers, flushMemoryLogger, removeLoggerHandlers


# NOTE: Not included in unittests
def crabCommand(command, *args, **kwargs):
    """ crabComand - executes a given command with certain arguments and returns
                     the raw result back from the client. Arguments are...
    """
    #Converting all arguments to a list. Adding '--' and '='
    arguments = []
    for key, val in kwargs.items():
        if isinstance(val, bool):
            if val:
                arguments.append('--'+str(key))
        else:
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
                  Every command returns a dictionary of the form
                  {'commandStatus': status, key: val, key: val ....}
                  where status can have the values 'SUCCESS' or 'FAILED'
                  and the other keys and values are command dependent !
    """
    tblogger, logger, memhandler = initLoggers()

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
        else:
        # We can reach here if the PSet raises a SystemExit exception
        # Without this, CRAB raises a confusing UnboundLocalError
            logger.error('PSet raised a SystemExit. Traceback follows:')
            logger.error(traceback.format_exc())
            raise
    finally:
        flushMemoryLogger(tblogger, memhandler, logger.logfile)
        removeLoggerHandlers(tblogger)
        removeLoggerHandlers(logger)
    return res
