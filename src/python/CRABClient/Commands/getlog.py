import os
import sys
import urllib
from httplib import HTTPException

from CRABClient import __version__
from RESTInteractions import HTTPRequests
from CRABClient.UserUtilities import getFileFromURL
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.Commands.getcommand import getcommand
from CRABClient.ClientUtilities import colors
from CRABClient.ClientExceptions import ConfigurationException, RESTCommunicationException, ClientException, MissingOptionException
import CRABClient.Emulator


class getlog(getcommand):
    """
    Retrieve the log files of a number of jobs specified by the -q/--quantity option.
    -q logfiles per exit code are returned if transferLogs = False; otherwise all the log files
    collected by the LogCollect job are returned. The task is identified by the -d/--dir option.
    """
    name = 'getlog'
    shortnames = ['log']
    visible = True #overwrite getcommand

    def __call__(self):
        if self.options.short:
            inputlist = {'subresource': 'webdir', 'workflow': self.cachedinfo['RequestName']}
            serverFactory = CRABClient.Emulator.getEmulator('rest')
            server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__) 
            uri = self.getUrl(self.instance, resource = 'task')
            dictresult, status, reason =  server.get(uri, data = inputlist)
            self.logger.info('Server result: %s' % dictresult['result'][0])
            dictresult = self.processServerResult(dictresult)
            if status != 200:
                msg = "Problem retrieving information from the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputlist), str(dictresult), str(reason))
                raise RESTCommunicationException(msg)
            self.setDestination()
            self.logger.info("Setting the destination to %s " % self.dest)
            self.logger.info("Retrieving...")
            success = []
            failed = []        
            for item in self.options.jobids:
                jobid = str(item[1])
                filename = 'job_out.'+jobid+'.0.txt'
                url = dictresult['result'][0]+'/'+filename
                try:
                    getFileFromURL(url, self.dest+'/'+filename)
                    self.logger.info ('Retrieved %s' % (filename))
                    success.append(filename)
                    retry = 1
                    #To retrieve retried joblog, if there is any.
                    while urllib.urlopen(dictresult['result'][0]+'/'+'job_out.'+jobid+'.'+str(retry)+'.txt').getcode() == 200:
                        filename = 'job_out.'+jobid+'.'+str(retry)+'.txt'
                        url = dictresult['result'][0]+'/'+filename
                        getFileFromURL(url, self.dest+'/'+filename)
                        self.logger.info ('Retrieved %s' % (filename))
                        success.append(filename)
                        retry = retry + 1
                except ClientException as ex:
                    self.logger.debug(str(ex))
                    failed.append(filename)
            if failed:
                msg = "%sError%s: Failed to retrieve the following files: %s" % (colors.RED,colors.NORMAL,failed)
                self.logger.info(msg)
            else:
                self.logger.info("%sSuccess%s: All files successfully retrieved." % (colors.GREEN,colors.NORMAL))
            returndict = {'success': success, 'failed': failed}
        else:
            returndict = getcommand.__call__(self, subresource = 'logs')
            if not returndict.get('success', {}) and not returndict.get('failed', {}):
                msg = "This is normal behavior unless General.transferLogs=True is present in the task configuration."
                self.logger.info(msg)
        return returndict

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '--quantity',
                                dest = 'quantity',
                                help = 'The number of logs you want to retrieve (or "all"). Ignored if --jobids is used.' )
        self.parser.add_option( '--parallel',
                                dest = 'nparallel',
                                help = 'Number of parallel download, default is 10 parallel download.',)
        self.parser.add_option( '--wait',
                                dest = 'waittime',
                                help = 'Increase the sendreceive-timeout in second.',)
        self.parser.add_option( '--short',
                                dest = 'short',
                                default = False,
                                action = 'store_true',
                                help = 'Get the short version of the log file. Use with --dir and --jobids.',)
        getcommand.setOptions(self)


    def validateOptions(self):
        getcommand.validateOptions(self)
        if self.options.short:
            if self.options.jobids is None:
                msg  = "%sError%s: Please specify the job ids for which to retrieve the logs." % (colors.GREEN, colors.NORMAL)
                msg += " Use the --jobids option."
                ex = MissingOptionException(msg)
                ex.missingOption = "jobids"
                raise ex
