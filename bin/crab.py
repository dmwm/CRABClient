"""
This contains the hooks to call the different command plug-ins.
It is not intended to contain any of the CRAB-3 client logic,
it simply:
  - intercepts the CLI options and command
  - loads and calls the specified command
  - exit with the proper exit codes
"""
from __future__ import print_function
from __future__ import division
import sys
import os

if not os.environ.get('CMSSW_VERSION',None):
    print('\nError: $CMSSW_VERSION is not defined. Make sure you do cmsenv first. Exiting...')
    sys.exit()

if sys.version_info < (2, 6):
    print('\nError: using a version of python < 2.6. Exiting...\n')
    sys.exit()

import pycurl
if not 'OpenSSL' in pycurl.version:
    print('\nError: missing SSL support in pycurl. Make sure you do cmsenv first. Exiting...')
    print('pycurl version is: %s\n' % pycurl.version)
    sys.exit()

if 'crab-dev' in __file__:
    print('BEWARE: this is the development version of CRAB Client.\nBe sure to have a good reason for using it\n')

import logging
import logging.handlers
import re

from ServerUtilities import FEEDBACKMAIL

from CRABClient.CRABOptParser import CRABOptParser
from CRABClient import __version__ as client_version
from CRABClient.ClientMapping import parametersMapping
from CRABClient.ClientExceptions import ClientException, RESTInterfaceException
from CRABClient.ClientUtilities import getAvailCommands, initLoggers, setConsoleLogLevelVar, StopExecution, flushMemoryLogger, LOGFORMATTER


class MyNullHandler(logging.Handler):
    """
    TODO: Python 2.7 supplies a null handler that will replace this.
    """
    def emit(self, record):
        """
        TODO: Python 2.7 supplies a null handler that will replace this.
        """
        pass  # pylint: disable=unnecessary-pass


class CRABClient(object):
    def __init__( self ):
        """
        Get the command to run, the options to pass it and a logger instance
        at appropriate level
        """

        self.subCommands = getAvailCommands()
        self.parser = CRABOptParser(self.subCommands)
        self.tblogger = None
        self.logger = None
        self.memhandler = None
        self.cmd = None

    def __call__(self):

        (options, args) = self.parser.parse_args()

        ## The default logfile destination is ./crab.log. It will be changed once we
        ## know/create the CRAB project directory.
        if options.quiet:
            setConsoleLogLevelVar(logging.WARNING)
        elif options.debug:
            setConsoleLogLevelVar(logging.DEBUG)
        self.tblogger, self.logger, self.memhandler = initLoggers()

        #Instructions needed in case of early failures: sometimes the traceback logger
        #has not been set yet.

        ## Will replace Python's sys.excepthook default function with the next function.
        ## This function is used for handling uncaught exceptions (in a Python program
        ## this happens just before the program exits).
        ## In this function:
        ## - make sure everything is logged to the crab.log file;
        ## However, we already have a `finally' clause where we make sure everything is
        ## logged to the crab log file.
        def log_exception(exc_type, exc_value, tback):
            """
            Send a short version of the exception to the console,
            a long version to the log

            Adapted from Doug Hellmann

            This might help sometimes:
            import traceback,pprint;
            pprint.pprint(traceback.format_tb(tback))
            """

            ## Add to the CRAB3 logger a file handler to the log file (if it doesn't have it
            ## already).
            tbLogger = logging.getLogger('CRAB3')
            hasFileHandler = False
            for h in tbLogger.handlers:
                if isinstance(h, logging.FileHandler) and h.stream.name == client.logger.logfile:
                    hasFileHandler = True
            if not hasFileHandler:
                filehandler = logging.FileHandler(client.logger.logfile)
                filehandler.setFormatter(LOGFORMATTER)
                tbLogger.addHandler(filehandler)
            ## This goes to the log file.
            tbLogger.error("Unhandled Exception!")
            tbLogger.error(exc_value, exc_info = (exc_type, exc_value, tback))

            ## This goes to the console (via the CRAB3.all logger) and to the log file (via
            ## the parent CRAB3 logger).
            logger = logging.getLogger('CRAB3.all')
            logger.error("ERROR: %s: %s", exc_type.__name__, exc_value)
            logger.error("\n\tPlease email %s for support with the crab.log file or crab.log URL.", FEEDBACKMAIL)
            logger.error("\tClient Version: %s", client_version)

            logger.error("\tPlease use 'crab uploadlog' to upload the log file %s to the CRAB cache.", client.logger.logfile)

        sys.excepthook = log_exception

        # check that the command is valid
        if len(args) == 0:
            print("You have not specified a command.")
            # Described the valid commands in epilog, reuse here
            print(self.parser.epilog)
            sys.exit(-1)

        sub_cmd = None
        try:
            sub_cmd = next( v for k,v in self.subCommands.items() if args[0] in v.shortnames or args[0]==v.name)
        except StopIteration:
            print("'" + str(args[0]) + "' is not a valid command.")
            self.parser.print_help()
            sys.exit(-1)
        self.cmd = sub_cmd(self.logger, args[1:])

        self.cmd()


if __name__ == "__main__":
    # Create the crab object and start it
    # Handled in a try/except to run in a controlled environment
    #  - do not want to expose known exception to the outside
    #  - exceptions thrown in the client should exit and set an approprate
    #    exit code, this is a safety net
    exitcode = 1
    client = CRABClient()
    schedInterv = "It seems the CMSWEB frontend is not responding. Please check: https://twiki.cern.ch/twiki/bin/viewauth/CMS/ScheduledInterventions?sortcol=3;table=1;up=2#sorted_table"
    try:
        client()
        exitcode = 0 #no exceptions no errors
    except RESTInterfaceException as err:
        exitcode=err.exitcode
        client.logger.info("The server answered with an error.")
        client.logger.debug("")
        err = str(err)
        if ("CMSWEB Error: Service unavailable") in err:
            client.logger.info(schedInterv)
        if 'X-Error-Detail' in err:
            errorDetail = re.search(r'(?<=X-Error-Detail:\s)[^\n]*', err).group(0)
            client.logger.info('Server answered with: %s', errorDetail)
        if 'X-Error-Info' in err:
            reason = re.search(r'(?<=X-Error-Info:\s)[^\n]*', err).group(0)
            for parname in parametersMapping['on-server']:
                tmpmsg = "'%s'" % (parname)
                if tmpmsg in reason and parametersMapping['on-server'][parname]['config']:
                    reason = reason.replace(tmpmsg, tmpmsg.replace(parname, ' or '.join(parametersMapping['on-server'][parname]['config'])))
            client.logger.info('Reason is: %s', reason)
        if 'X-Error-Id' in err:
            errorId = re.search(r'(?<=X-Error-Id:\s)[^\n]*', err).group(0)
            client.logger.info('Error Id: %s', errorId)
        logging.getLogger('CRAB3').exception('Caught RESTInterfaceException exception')
    except pycurl.error as pe:
        client.logger.error(pe)
        logging.getLogger('CRAB3').exception('Caught pycurl.error exception')
        exitcode = pe.args[0]
        if pe[1].find('(DNS server returned answer with no data)'):
            client.logger.info(schedInterv)
    except ClientException as ce:
        client.logger.error(ce)
        logging.getLogger('CRAB3').exception('Caught ClientException exception')
        exitcode = ce.exitcode
    except KeyboardInterrupt:
        client.logger.error('Keyboard Interrupted')
    except StopExecution:
        exitcode = 0
    finally:
        # the command crab --version does not have a logger instance
        if getattr(client, 'tblogger', None) and getattr(client, 'memhandler', None) and getattr(client, 'logger', None):
            flushMemoryLogger(client.tblogger, client.memhandler, client.logger.logfile)

    if getattr(client, 'cmd', None):
        # this will also print out location of log file
        client.cmd.terminate( exitcode )
    else:
        client.logger.info('Log file is %s', client.logger.logfile)

    sys.exit( exitcode )

