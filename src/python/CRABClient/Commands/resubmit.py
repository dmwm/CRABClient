from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_exceptions import ConfigException, RESTCommunicationException
from CRABClient.client_utilities import validateJobids
from CRABClient import __version__

from RESTInteractions import HTTPRequests

from urllib import urlencode
import re


class resubmit(SubCommand):
    """ Resubmit the failed jobs of the task identified by
    -t/--task option
    """
    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.debug('Requesting resubmission for failed jobs in task %s' % self.cachedinfo['RequestName'] )
        configreq = { 'workflow' : self.cachedinfo['RequestName']}

        for attr in ['maxmemory', 'maxjobruntime', 'numcores', 'priority']:
            val = getattr(self, attr, None)
            if val:
                configreq[attr] = val

        dictresult, status, reason = server.post(self.uri, data = urlencode({ 'workflow' : self.cachedinfo['RequestName']}) + \
                                                    self.sitewhitelist + self.siteblacklist + '&' + urlencode(self.jobids))
        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving resubmitting the task to the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputdict), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        self.logger.info("Resubmit request successfully sent")
        if dictresult['result'][0]['result'] != 'ok':
            self.logger.info(dictresult['result'][0]['result'])

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "--blacklist",
                                 dest = 'siteblacklist',
                                 default = None,
                                 help = "Set the sites you want to blacklist during the resubmission." + \
                                            " Comma separated list of cms sites (e.g.: T2_ES_CIEMAT,T2_IT_Rome[...]).")

        self.parser.add_option( "--whitelist",
                                 dest = 'sitewhitelist',
                                 default = None,
                                 help = "Set the sites you want to whitelist during the resubmission." + \
                                            " Comma separated list of cms sites (e.g.: T2_ES_CIEMAT,T2_IT_Rome[...]).")

        self.parser.add_option( "--memory",
                                 dest = 'maxmemory',
                                 default = None,
                                 type = "int",
                                 help = "Set the maximum memory used per job in this task." + \
                                            " This is in units of MB (e.g.: 2000 for 2GB of RAM).")

        self.parser.add_option( "--cores",
                                 dest = 'numcores',
                                 default = None,
                                 type = "int",
                                 help = "Set the number of cores used per job in this task." + \
                                            " (e.g.: 1 for single-threaded applications).")

        self.parser.add_option( "--priority",
                                 dest = 'priority',
                                 default = None,
                                 type = "int",
                                 help = "Set the priority of this task compared to other tasks you own; tasks default to 10." + \
                                            " This does not improve your share compared to other users.")

        self.parser.add_option( "--wall",
                                 dest = 'maxjobruntime',
                                 default = None,
                                 type = "int",
                                 help = "Set the maximum time, in hours, jobs in this task are allowed to run." + \
                                            " Default is 24 hours.")

        self.parser.add_option( '--jobids',
                                dest = 'jobids',
                                default = None,
                                help = 'Ids of the jobs you want to resubmit. Comma separated list of integers',
                                metavar = 'JOBIDS' )

    def validateOptions(self):
        """
        Check if the sitelist parameter is a comma separater list of cms sitenames,
        and put the strings to be passed to the server to self
        """
        SubCommand.validateOptions(self)

        #Checking if the sites provided by the user are valid cmsnames. Doing this because with only the
        #server error handling we get:
        #    Server answered with: Invalid input parameter
        #    Reason is: Incorrect 'siteblacklist' parameter
        #which is not really user friendly.
        #Moreover, I prefer to be independent from Lexicon. I'll the regex here.
        sn_re = "^T[1-3]_[A-Z]{2}(_[A-Za-z0-9]+)+$" #sn_re => SiteName_RegularExpression
        sn_rec = re.compile(sn_re) #sn_rec => SiteName_RegularExpressionCompiled

        self.sitewhitelist = ''
        self.siteblsklist = ''

        for siteList in ['sitewhitelist', 'siteblacklist']:
            result = ''
            paramVal = getattr(self.options, siteList, None)
            if paramVal:
                for site in paramVal.split(','):
                    if not sn_rec.match(site):
                        raise ConfigException("The sitename %s dows not look like a valid CMS name (not matching %s)" % (site, sn_re) )
                    result += "&%s=%s" % (siteList, site)
            setattr(self, siteList, result)

        #check the format of jobids
        self.jobids = ''
        if getattr(self.options, 'jobids', None):
            self.jobids = validateJobids(self.options.jobids)

        # Sanity checks for task sizes.  Limits are purposely fairly generous to provide some level of future-proofing.
        # The server may restrict further.
        self.numcores = None
        if self.options.numcores != None:
            if self.options.numcores < 1 or self.options.numcores > 128:
                raise ConfigException("The number of requested cores (%d) must be between 1 and 128." % (self.options.numcores))
            self.numcores = str(self.options.numcores)

        self.maxjobruntime = None
        if self.options.maxjobruntime != None:
            if self.options.maxjobruntime < 1 or self.options.maxjobruntime > 336:
                raise ConfigException("The requested max job runtime (%d hours) must be between 1 and 336 hours." % self.options.maxjobruntime)
            self.maxjobruntime = str(self.options.maxjobruntime)

        self.maxmemory = None
        if self.options.maxmemory != None:
            if self.options.maxmemory < 30 or self.options.maxmemory > 1024*30:
                raise ConfigException("The requested per-job memory (%d MB) must be between 30 and 30720 MB." % self.options.maxmemory)
            self.maxmemory = str(self.options.maxmemory)

        self.priority = None
        if self.options.priority != None:
            self.priority = str(self.options.priority)

