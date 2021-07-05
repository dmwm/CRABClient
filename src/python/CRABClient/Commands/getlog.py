from __future__ import print_function
from __future__ import division

try:
    from http.client import HTTPException  # Python 3 and Python 2 in modern CMSSW
except:  # pylint: disable=bare-except
    from httplib import HTTPException  # old Python 2 version in CMSSW_7

from CRABClient.ClientUtilities import colors, validateJobids, getColumn
from CRABClient.UserUtilities import curlGetFileFromURL
from CRABClient.Commands.getcommand import getcommand
from CRABClient.ClientExceptions import RESTCommunicationException, MissingOptionException

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

    def __call__(self):  # pylint: disable=arguments-differ
        if self.options.short:
            taskname = self.cachedinfo['RequestName']
            inputlist = {'subresource': 'search', 'workflow': taskname}
            server = self.crabserver
            webdir = getProxiedWebDir(crabserver=self.crabserver, task=taskname, logFunction=self.logger.debug)
            dictresult, status, reason = server.get(api='task', data=inputlist)
            if not webdir:
                webdir = dictresult['result'][0]
                self.logger.info('Server result: %s' % webdir)
                if status != 200:
                    msg = "Problem retrieving information from the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputlist), str(dictresult), str(reason))
                    raise RESTCommunicationException(msg)
            splitting = getColumn(dictresult, 'tm_split_algo')
            if getattr(self.options, 'jobids', None):
                self.options.jobids = validateJobids(self.options.jobids, splitting != 'Automatic')
            self.setDestination()
            self.logger.info("Setting the destination to %s " % self.dest)
            failed, success = self.retrieveShortLogs(webdir, self.proxyfilename)
            if failed:
                msg = "%sError%s: Failed to retrieve the following files: %s" % (colors.RED, colors.NORMAL, failed)
                self.logger.info(msg)
            else:
                self.logger.info("%sSuccess%s: All files successfully retrieved." % (colors.GREEN, colors.NORMAL))
            returndict = {'success': success, 'failed': failed}
        else:
            # Different from the old getlog code: set 'logs2' as subresource so that 'getcommand' uses the new logic.
            returndict = getcommand.__call__(self, subresource='logs2')
            if ('success' in returndict and not returndict['success']) or \
               ('failed' in returndict and returndict['failed']):
                msg = "You can use the --short option to retrieve a short version of the log files from the Grid scheduler."
                self.logger.info(msg)

        return returndict


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option('--quantity',
                               dest='quantity',
                               help='The number of logs you want to retrieve (or "all"). Ignored if --jobids is used.')
        self.parser.add_option('--parallel',
                               dest='nparallel',
                               help='Number of parallel download, default is 10 parallel download.',)
        self.parser.add_option('--wait',
                               dest='waittime',
                               help='Increase the sendreceive-timeout in second.',)
        self.parser.add_option('--short',
                               dest='short',
                               default=False,
                               action='store_true',
                               help='Get the short version of the log file. Use with --dir and --jobids.',)
        getcommand.setOptions(self)


    def validateOptions(self):
        getcommand.validateOptions(self)
        if self.options.short:
            if self.options.jobids is None:
                msg = "%sError%s: Please specify the job ids for which to retrieve the logs." % (colors.GREEN, colors.NORMAL)
                msg += " Use the --jobids option."
                ex = MissingOptionException(msg)
                ex.missingOption = "jobids"
                raise ex


    def retrieveShortLogs(self, webdir, proxyfilename):
        self.logger.info("Retrieving...")
        success = []
        failed = []
        for _, jobid in self.options.jobids:
            # We don't know a priori how many retries the job had. So we start with retry 0
            # and increase it by 1 until we are unable to retrieve a log file (interpreting
            # this as the fact that we reached the highest retry already).
            retry = 0
            succeded = True
            while succeded:
                filename = 'job_out.%s.%s.txt' % (jobid, retry)
                url = webdir + '/' + filename
                httpCode = curlGetFileFromURL(url, self.dest + '/' + filename, proxyfilename, logger=self.logger)
                if httpCode == 200:
                    self.logger.info('Retrieved %s' % (filename))
                    success.append(filename)
                    retry += 1  # To retrieve retried job log, if there is any.
                elif httpCode == 404:
                    succeded = False
                    # Ignore the exception if the HTTP status code is 404. Status 404 means file
                    # not found (see http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html). File
                    # not found error is expected, since we try all the job retries.
                else:
                    # something went wront in trying to retrieve a file which was expected to be there
                    succeded = False
                    failed.append(filename)

        return failed, success

