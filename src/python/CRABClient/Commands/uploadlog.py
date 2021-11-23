import os

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientUtilities import colors, uploadlogfile
from CRABClient.ClientExceptions import ConfigurationException, MissingOptionException


class uploadlog(SubCommand):
    """
    Upload the crab.log file (or any "log" file) to the CRAB User File Cache.
    The main purpose of this command is to make log files available to experts for
    debugging. The command accepts a path to a CRAB project directory via the
    -d/--dir option, in which case it will search for the crab.log file inside the
    directory. The log will be uplaoded in S3 in the task directory, if a task name
    is not available (crab submit fails in early stage) user will have to send the
    log in the mail. It will be a short log in that case.
    Usage:
      * crab uploadlog --dir=<path-to-a-CRAB-project-directory>
    """
    name = 'uploadlog'
    shortnames = ['uplog']

    def __call__(self):
        self.logger.debug("uploadlog started")
        taskname = None
        #veryfing the log file exist
        if os.path.isfile(self.logfile):
            self.logger.debug("crab.log exists")
            try:
                taskname = self.cachedinfo['RequestName']
                logfilename = str(taskname)+".log"
            except Exception:
                self.logger.info("Couldn't get information from .requestcache (file likely not created due to submission failure),\n" +
                                 "Please local crab.log yourself and copy/paste into the mail to support if needed")
                return {}
        else:
            msg = "%sError%s: Could not locate log file." % (colors.RED, colors.NORMAL)
            self.logger.info(msg)
            raise ConfigurationException

        self.logger.info("Will upload file %s." % (self.logfile))
        logfileurl = uploadlogfile(self.logger, self.proxyfilename, taskname=taskname, logfilename=logfilename,
                                   logpath=str(self.logfile), instance=self.instance,
                                   serverurl=self.serverurl)
        return {'result': {'status': 'SUCCESS', 'logurl': logfileurl}}


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        return


    def validateOptions(self):
        ## Do the options validation from SubCommand.
        try:
            SubCommand.validateOptions(self)
        except MissingOptionException as ex:
            if ex.missingOption == "task":
                msg = "%sError%s:" % (colors.RED, colors.NORMAL)
                msg += " Please provide a path to a CRAB project directory (use the -d/--dir option)."
                ex = MissingOptionException(msg)
                ex.missingOption = "task"
            raise ex
