import re
import pickle
import os
import sys

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientUtilities import colors, PKL_W_MODE
from CRABClient.ClientUtilities import commandUsedInsideCrab
from CRABClient.ClientExceptions import MissingOptionException,ConfigurationException


class remake(SubCommand):
    """
    Remake the .requestcache
    """
    name = 'remake'
    shortnames = ['rmk']

    def __call__(self):
        if not commandUsedInsideCrab():
            msg = "ATTENTION return value for 'remake' has been changed to a dictionary"
            msg += "\n format is {'commandStatus': 'SUCCESS' or 'FAILED',"
            msg += "\n            'workDir': name of the work directory created}"
            self.logger.warning(msg)
        return self.remakecache(''.join(self.options.cmptask.split()))

    def remakecache(self,taskname):
        requestarea = taskname.split(":", 1)[1].split("_", 1)[1]
        cachepath = os.path.join(requestarea, '.requestcache')
        if os.path.exists(cachepath):
            self.logger.info("%sError%s: %s not created, because it already exists." % (colors.RED, colors.NORMAL, cachepath))
        elif not os.path.exists(requestarea):
            self.logger.info('Remaking %s folder.' % (requestarea))
            try:
                os.mkdir(requestarea)
                os.mkdir(os.path.join(requestarea, 'results'))
                os.mkdir(os.path.join(requestarea, 'inputs'))
            except IOError:
                self.logger.info("%sWarning%s: Failed to make a request area." % (colors.RED, colors.NORMAL))
            self.logger.info("Remaking .requestcache file.")
            dumpfile = open(cachepath , PKL_W_MODE)
            pickle.dump({'voGroup': '', 'Server': self.serverurl , 'instance': self.instance,
                         'RequestName': taskname, 'voRole': '', 'Port': ''}, dumpfile, protocol=0)
            dumpfile.close()
            self.logger.info("%sSuccess%s: Finished remaking project directory %s" % (colors.GREEN, colors.NORMAL, requestarea))

        returnDict = {'commandStatus': 'SUCCESS', 'workDir': requestarea}
        return returnDict

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option("--task",
                               dest = "cmptask",
                               default = None,
                               help = "The complete task name. Can be taken from 'crab status' output, or from dashboard.")


    def validateOptions(self):
        if self.options.cmptask is None:
            msg  = "%sError%s: Please specify the task name for which to remake a CRAB project directory." % (colors.RED, colors.NORMAL)
            msg += " Use the --task option."
            ex = MissingOptionException(msg)
            ex.missingOption = "cmptask"
            raise ex
        else:
            regex = "^\d{6}_\d{6}_?([^\:]*)\:[a-zA-Z0-9-]+_(crab_)?.+"
            if not re.match(regex, self.options.cmptask):
                msg = "%sError%s: Task name does not match the regular expression '%s'." % (colors.RED, colors.NORMAL, regex)
                raise ConfigurationException(msg)
