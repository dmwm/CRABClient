from CRABClient.Commands import CommandResult, mergeResults
from CRABClient.Commands.remote_copy import remote_copy
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.Commands.getcommand import getcommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import ConfigurationException
import os

class getlog(getcommand):
    """ Retrieve the log files of a number of jobs specified by the -q/--quantity option.
-q logfiles per exit code are returned if SaveLogs=False, otherwise all the logfiles
collected by the LogCollect job are returned.
The task is identified by -t/--task option
    """
    name = 'getlog'
    shortnames = ['log']
    visible = True #overwrite getcommand

    def __call__(self):
        getcommand.__call__(self, subresource = 'logs')


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '-q', '--quantity',
                                dest = 'quantity',
                                default = 1,
                                help = 'A number which express the number of files you want to retrieve. Defaut one log per exitcode' )
        getcommand.setOptions(self)

    def processServerResult(self, result):
        newresult = []
        saveLog = self._hasLogCollect(result)

        for file in result['result']:
            #drop items with no pfn and error (server uses return the error message)
            if not 'pfn' in file and 'error' in file:
                self.logger.info("Cannot find the log of jobs with exitcode %s: %s" % (file['exitcode'],file['error']))
                continue
            #append the file if the task has no output from logcollectjobs
            if not saveLog:
                #set the suffix of the file (exitcode)
                file['suffix'] = "ec" + str(file['exitcode'])
                newresult.append(file)
            #otherwise only append logcollect output (the big tar.gz)
            elif file['type']=='logCollect':
               newresult.append(file)

        return {'result' : newresult}

    def _hasLogCollect(self, result):
        for file in result['result']:
            if 'type' in file and file['type'] in 'logCollect':
                return True
        return False
