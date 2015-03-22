import os
import time

from CRABClient.Commands.SubCommand import SubCommand
from WMCore.Services.UserFileCache.UserFileCache import UserFileCache
from CRABClient.ClientUtilities import colors, server_info, uploadlogfile
from CRABClient.ClientExceptions import ConfigurationException, MissingOptionException


class uploadlog(SubCommand):
    """
    Upload the crab.log file (or any "log" file) to the CRAB User File Cache.
    The main purpose of this command is to make log files available to experts for
    debugging. The command accepts a path to a CRAB project directory via the
    -d/--dir option, in which case it will search for the crab.log file inside the
    directory, or a path to a "log" file via the --logpath option, particularly
    needed to upload a crab.log file from the current working directory when a
    CRAB project directory was not created. (If both options are specified, the
    --logpath option takes precedence.)
    Usage:
      * crab uploadlog --dir=<path-to-a-CRAB-project-directory>
      * crab uploadlog --logpath=<path-to-a-log-file>
    """
    name = 'uploadlog'
    shortnames = ['uplog']

    def __call__(self):
        self.logger.debug("uploadlog started")
        #veryfing the log file exist
        if self.options.logpath is not None:
            logfilename = str(time.strftime("%Y-%m-%d_%H%M%S"))+'_crab.log'
            self.logfile = self.options.logpath
        elif os.path.isfile(self.logfile):
            self.logger.debug("crab.log exists")
            logfilename = str(self.cachedinfo['RequestName'])+".log"
        else:
            msg = "%sError%s: Could not locate log file." % (colors.RED, colors.NORMAL)
            self.logger.info(msg)
            raise ConfigurationException

        self.logger.info("Will upload file %s." % (self.logfile))
        logfileurl = uploadlogfile(self.logger, self.proxyfilename, logfilename = logfilename, \
                                   logpath = str(self.logfile), instance = self.instance, \
                                   serverurl = self.serverurl)
        return {'result' : {'status' : 'SUCCESS' , 'logurl' : logfileurl}}


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option("--logpath",
                               dest = "logpath",
                               default = None,
                               help = "Path to the log file to be uploaded.")


    def validateOptions(self):

        ## Check if the --logpath option was used. If it was, don't require the task
        ## option.
        if self.options.logpath is not None:
            if not os.path.isfile(self.options.logpath):
                msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
                msg += " Could not find the log file %s." % (self.options.logpath)
                raise ConfigurationException(msg)
            self.cmdconf['requiresTaskOption'] = False

        ## Do the options validation from SubCommand.
        try:
            SubCommand.validateOptions(self)
        except MissingOptionException, ex:
            if ex.missingOption == "task":
                msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
                msg += " Please provide a path to a CRAB project directory (use the -d/--dir option)"
                msg += " or to a log file (use the --logpath option)."
                ex = MissingOptionException(msg)
                ex.missingOption = "task"
            raise ex
