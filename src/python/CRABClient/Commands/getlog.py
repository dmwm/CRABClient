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
                                help = 'A number which express the number of files you want to retrieve (or all). Defaut one log per exitcode' )
        getcommand.setOptions(self)

    def processServerResult(self, result):
        newresult = []
        saveLog = self._hasLogCollect(result)

        #prepare the list of exit codes found
        foundExitCodes = [str(myfile['exitcode']) for myfile in result['result'] if 'type' in myfile and myfile['type'] in 'logArchive']
        for myfile in result['result']:
            if 'missing' in myfile:
                #tell the user if the server did not find a log for any of the missing exit codes
                for ec in set(myfile['missing'].keys()) - set(foundExitCodes):
                    self.logger.info("Cannot find a log for exit code %s" % ec)
                continue
            #append the file if the task has no output from logcollectjobs
            if not saveLog:
                #set the suffix of the file (exitcode)
                myfile['suffix'] = "ec" + str(myfile['exitcode'])
                newresult.append(myfile)
            #otherwise only append logcollect output (the big tar.gz)
            elif myfile['type']=='logCollect':
               newresult.append(myfile)

        return {'result' : newresult}

    def _hasLogCollect(self, result):
        for myfile in result['result']:
            if 'type' in myfile and myfile['type'] == 'logCollect':
                return True
        return False
