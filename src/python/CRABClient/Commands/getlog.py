from __future__ import print_function
from __future__ import division

import CRABClient.Emulator
from CRABClient import __version__
from CRABClient.ClientUtilities import colors
from CRABClient.UserUtilities import getFileFromURL
from CRABClient.Commands.getcommand import getcommand
from CRABClient.ClientExceptions import RESTCommunicationException, ClientException, MissingOptionException

from ServerUtilities import getProxiedWebDir


class getlog(getcommand):
    """
    Important: code here is identical to the old getlog implementation (aside from setting the subresource to
    'logs2' when calling getcommand and the names of the command/class themselves). This was done because trying to
    avoid copy-paste code isn't worth the effort in this case. When the status2 is working correctly, old code will
    be easily removed and replaced with the 'getlog2' version. Also, the command 'getlog' itself is deprecated and
    we don't expect to make any changes to it until it's removed.

    Class description:
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
            # Different from the old getlog code: set 'logs2' as subresource so that 'getcommand' uses the new logic.
            returndict = getcommand.__call__(self, subresource = 'logs2')
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
            ## We don't know a priori how many retries the job had. So we start with retry 0
            ## and increase it by 1 until we are unable to retrieve a log file (interpreting
            ## this as the fact that we reached the highest retry already).
            retry = 0
            succeded = True
            while succeded:
                filename = 'job_out.%s.%s.txt' % (jobid, retry)
                url = webdir + '/' + filename
                try:
                    getFileFromURL(url, self.dest + '/' + filename, proxyfilename)
                    self.logger.info('Retrieved %s' % (filename))
                    success.append(filename)
                    retry += 1 #To retrieve retried job log, if there is any.
                except ClientException as ex:
                    succeded = False
                    ## Ignore the exception if the HTTP status code is 404. Status 404 means file
                    ## not found (see http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html). File
                    ## not found error is expected, since we try all the job retries.
                    if not hasattr(ex, "status") or ex.status!=404:
                        self.logger.debug(str(ex))
                        failed.append(filename)

        return failed, success

