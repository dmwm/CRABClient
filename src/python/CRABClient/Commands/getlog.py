import urllib

import CRABClient.Emulator
from CRABClient import __version__
from CRABClient.ClientUtilities import colors
from CRABClient.UserUtilities import getFileFromURL
from CRABClient.Commands.getcommand import getcommand
from CRABClient.ClientExceptions import RESTCommunicationException, ClientException, MissingOptionException

from ServerUtilities import getProxiedWebDir

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
            taskname = self.cachedinfo['RequestName']
            inputlist = {'subresource': 'webdir', 'workflow': taskname}
            serverFactory = CRABClient.Emulator.getEmulator('rest')
            server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)
            uri = self.getUrl(self.instance, resource = 'task')
            webdir = getProxiedWebDir(taskname, self.serverurl, uri, self.proxyfilename, self.logger.debug)
            if not webdir:
                dictresult, status, reason =  server.get(uri, data = inputlist)
                webdir = dictresult['result'][0]
                self.logger.info('Server result: %s' % webdir)
                if status != 200:
                    msg = "Problem retrieving information from the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputlist), str(dictresult), str(reason))
                    raise RESTCommunicationException(msg)
            self.setDestination()
            self.logger.info("Setting the destination to %s " % self.dest)
            failed, success = self.retrieveShortLogs(webdir, self.proxyfilename)
            if failed:
                msg = "%sError%s: Failed to retrieve the following files: %s" % (colors.RED,colors.NORMAL,failed)
                self.logger.info(msg)
            else:
                self.logger.info("%sSuccess%s: All files successfully retrieved." % (colors.GREEN,colors.NORMAL))
            returndict = {'success': success, 'failed': failed}
        else:
            returndict = getcommand.__call__(self, subresource = 'logs')
            if ('success' in returndict and not returndict['success']) or \
               ('failed'  in returndict and returndict['failed']):
                msg = "You can use the --short option to retrieve a short version of the log files from the Grid scheduler."
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


    def retrieveShortLogs(self, webdir, proxyfilename):
        self.logger.info("Retrieving...")
        success = []
        failed = []
        for _, jobid in self.options.jobids:
            retry = 0
            succeded = True
            while succeded:
                filename = 'job_out.%s.%s.txt' % (jobid, retry)
                url = webdir + '/' + filename
                try:
                    getFileFromURL(url, self.dest + '/' + filename, proxyfilename)
                    self.logger.info('Retrieved %s' % (filename))
                    success.append(filename)
                    retry += 1 #To retrieve retried joblog, if there is any.
                except ClientException as ex:
                    succeded = False
                    #only print exception that are not 404, this is expected since we try all the jobs retries
                    if not hasattr(ex, "status") or ex.status!=404:
                        self.logger.debug(str(ex))
                    failed.append(filename)

        return failed, success

