from os import path
from CRABClient.Commands.SubCommand import SubCommand
from WMCore.Services.UserFileCache.UserFileCache import UserFileCache
from CRABClient.client_utilities import colors
from CRABClient.client_utilities import server_info
from CRABClient.client_exceptions import ConfigurationException
from CRABClient.client_utilities import uploadlogfile

import time

class uploadlog(SubCommand):
    """
    Upload the user log to the crab user file cache
    """
    name = 'uploadlog'
    shortnames = ['uplog']

    def __call__(self):
        self.logger.debug("uploadlog started")
        #veryfing the log file exist
        if hasattr(self.options, 'logpath') and self.options.logpath != None:
            logfilename = str(time.strftime("%Y-%m-%d_%H%M%S"))+'_crab.log'
            self.logfile = self.options.logpath
        elif path.isfile(self.logfile):
            self.logger.debug("crab.log exists")
            logfilename=str(self.cachedinfo['RequestName'])+".log"
        else:
            self.logger.info("%sError:%s Could not locate log file" % (colors.RED, colors.NORMAL))
            raise ConfigurationException

        logfileurl = uploadlogfile(self.logger, self.proxyfilename, logfilename = logfilename, logpath = str(self.logfile), instance = self.instance, serverurl = self.serverurl)


        return {'result' : {'status' : 'SUCCESS' , 'logurl' : logfileurl}}

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """

        self.parser.add_option( "--logpath",
                                 dest = "logpath",
                                 help = "Specify the log path file",
                                 )

        self.parser.add_option( "-t", "--task",
                                     dest = "task",
                                     default = None,
                                     help = "Same as -c/-continue" )


    def validateOptions(self):
        SubCommand.validateOptions(self)

        if hasattr(self.options, 'logpath') and self.options.logpath != None:
            if not path.isfile(self.options.logpath):
                msg = '%sError%s: Could not find the log file in the path: %s' % (colors.RED,colors.NORMAL,self.options.logpath)
                raise ConfigurationException(msg)
        elif hasattr(self.options, 'task'):
            self.cmdconf['requiresTaskOption'] = True
            self.cmdconf['useCache'] = True
            if self.options.task == None:
                if len(self.args) == 1 and self.args[0]:
                    self.options.task = self.args[0]
                #for the case of 'crab uploadlog', user is using the .crab3 file
                if self.options.task == None and hasattr(self, 'crab3dic'):
                    if  self.crab3dic["taskname"] != None:
                        self.options.task = self.crab3dic["taskname"]
                    else:
                        msg = '%sError%s: Please use the task option -t or --logpath to specify which log to upload' % (colors.RED, colors.NORMAL)
                        raise ConfigurationException(msg)
        else:
            msg = '%sError%s: Please use the task option -t or --logpath to specify which log to upload' % (colors.RED, colors.NORMAL)
            raise ConfigurationException(msg)
