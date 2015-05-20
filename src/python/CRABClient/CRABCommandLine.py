#!/usr/bin/env python
"""
This contains the hooks to call the different command plug-ins.
It is not intended to contain any of the CRAB-3 client logic,
it simply:
  - intercepts the CLI options and command
  - loads and calls the specified command
  - exit with the proper exit codes
"""
import sys
if sys.version_info < (2, 6):
    print '\nError: using a version of python < 2.6. Exiting...\n'
    sys.exit()

import os
import json
import pycurl
import logging
import logging.handlers

from httplib import HTTPException

from CRABClient.CRABOptParser import CRABOptParser
from CRABClient import __version__ as client_version
from CRABClient.ClientUtilities import getAvailCommands, logfilter , uploadlogfile, getLoggers, StopExecution
from CRABClient.ClientExceptions import ClientException
from CRABClient.ClientMapping import parametersMapping


class MyNullHandler(logging.Handler):
    """
    TODO: Python 2.7 supplies a null handler that will replace this.
    """
    def emit(self, record):
        """
        TODO: Python 2.7 supplies a null handler that will replace this.
        """
        pass


class CRABClient(object):
    def __init__( self ):
        """
        Get the command to run, the options to pass it and a logger instance
        at appropriate level
        """

        self.subCommands = getAvailCommands()
        self.parser = CRABOptParser(self.subCommands)


    def __call__(self):

        (options, args) = self.parser.parse_args()

        #the default the logfile destination is /crab.log, it will be  changed  when we have loaded the taskname
        loglevel = logging.INFO
        if options.quiet:
            loglevel = logging.WARNING
        if options.debug:
            loglevel = logging.DEBUG
        self.logger, self.memhandler = getLoggers(loglevel)


        #Instructions needed in case of early failures: sometimes the traceback logger
        #has not been set yet. The right file handler is only added when the workarea
        #of the task is created

        # Replace excepthook with logger, unexpected exception will go to finally first then to log_exception
        def log_exception(exc_type, exc_value, tback):
            """
            Send a short version of the exception to the console,
            a long version to the log

            Adapted from Doug Hellmann
            """
            ## Feedback email address
            feedbackemail = 'hn-cms-computing-tools@cern.ch'

            #adding a file handler
            clientinstance = self
            filehandler = logging.FileHandler(client.logger.logfile)
            logging.getLogger('CRAB3').addHandler(filehandler)

            ## This goes to the console.
            logging.getLogger('CRAB3.all').error("ERROR: %s: %s" % (exc_type.__name__, exc_value))
            logging.getLogger('CRAB3.all').error("\n\tPlease email %s for support with the crab.log file or crab.log URL." % (feedbackemail))
            logging.getLogger('CRAB3.all').error("\tClient Version: %s" % client_version)
            ## This goes to the crab log file.
            tbLogger = logging.getLogger('CRAB3')
            tbLogger.error("Unhandled Exception!")
            tbLogger.error(exc_value, exc_info = (exc_type, exc_value, tback))

            ## Uploading the crab log file.
            if hasattr(clientinstance, 'cmd') and hasattr(clientinstance.cmd, 'proxyfilename') and clientinstance.cmd.proxyfilename != None:
                try:
                    logurl = uploadlogfile(tbLogger, clientinstance.cmd.proxyfilename, logfilename = None, \
                                           logpath = str(client.logger.logfile), instance = 'prod')
                    msg  = "\tThe log file %s has been uploaded automatically." % (client.logger.logfile)
                    msg += "\n\tPlease email the following URL '%s' to %s." % (logurl, feedbackemail)
                    logging.getLogger('CRAB3.all').error(msg)
                except Exception:
                    logging.getLogger('CRAB3.all').debug('Failed to upload log file automatically')
                    logging.getLogger('CRAB3.all').error('\tPlease use crab uploadlog to upload the log file %s' % (client.logger.logfile))
            else:
                logging.getLogger('CRAB3.all').error('\tPlease use crab uploadlog to upload the log file %s' % (client.logger.logfile))

        sys.excepthook = log_exception

        # check that the command is valid
        if len(args) == 0:
            print "You have not specified a command."
            # Described the valid commands in epilog, reuse here
            print self.parser.epilog
            sys.exit(-1)

        sub_cmd = None
        try:
            sub_cmd = next( v for k,v in self.subCommands.items() if args[0] in v.shortnames or args[0]==v.name)
        except StopIteration:
            print "'" + str(args[0]) + "' is not a valid command."
            print self.parser.print_help()
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
    try:
        client()
        exitcode = 0 #no exceptions no errors
    except HTTPException, he:
        client.logger.info("Error contacting the server.")
        if he.status==503 and he.result.find("CMSWEB Error: Service unavailable")!=-1:
            client.logger.info("It seems the CMSWEB frontend is not responding. Please check: https://twiki.cern.ch/twiki/bin/viewauth/CMS/ScheduledInterventions")
        if he.headers.has_key('X-Error-Detail'):
            client.logger.info('Server answered with: %s' % he.headers['X-Error-Detail'])
        if he.headers.has_key('X-Error-Info'):
            reason = he.headers['X-Error-Info']
            for parname in parametersMapping['on-server']:
                for tmpmsg in ['\''+parname+'\' parameter', 'Parameter \''+parname+'\'']:
                    if tmpmsg in reason and parametersMapping['on-server'][parname]['config']:
                        reason = reason.replace(tmpmsg,tmpmsg.replace(parname, ' or '.join(parametersMapping['on-server'][parname]['config'])))
                        break
                else:
                    continue
                break
            client.logger.info('Reason is: %s' % reason)
        #The following goes to the logfile.
        errmsg = "ERROR: %s (%s): " % (he.reason, he.status)
        ## answer can be a json or not
        try:
            errmsg += " '%s'" % he.result
        except ValueError:
            pass
        client.logger.debug(errmsg)
        client.logger.debug('Command failed with URI: %s' % he.url)
        client.logger.debug('     Input data: %s' % he.req_data)
        client.logger.debug('     Request headers: %s' % he.headers)
        logging.getLogger('CRAB3').exception('Caught exception')
        exitcode = he.status
    except pycurl.error, pe:
        client.logger.error(pe)
        logging.getLogger('CRAB3').exception('Caught exception')
        exitcode = pe.args[0]
        if pe[1].find('(DNS server returned answer with no data)'):
            client.logger.info("It seems the CMSWEB frontend is not responding. Please check: https://twiki.cern.ch/twiki/bin/viewauth/CMS/ScheduledInterventions")
    except ClientException, ce:
        client.logger.error(ce)
        logging.getLogger('CRAB3').exception('Caught exception')
        exitcode = ce.exitcode
    except KeyboardInterrupt:
        client.logger.error('Keyboard Interrupted')
    except StopExecution:
        exitcode = 0
    finally:

        # the command crab --version do not have a logger instance
        if hasattr(client, 'logger'):
            filehandler = logging.FileHandler(client.logger.logfile)
            ff = logging.Formatter("%(levelname)s %(asctime)s: \t %(message)s")
            filehandler.setFormatter(ff)
            filehandler.setLevel(logging.DEBUG)
            logging.getLogger('CRAB').addHandler(filehandler)

            client.memhandler.setTarget(filehandler)
            client.memhandler.flush()
            client.memhandler.close()
            client.logger.removeHandler(client.memhandler)

    if hasattr(client, 'cmd'):
        client.cmd.terminate( exitcode )

    sys.exit( exitcode )
