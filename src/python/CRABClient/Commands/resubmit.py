from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_exceptions import ConfigurationException, RESTCommunicationException
from CRABClient.client_utilities import validateJobids
from CRABClient import __version__
import CRABClient.Emulator

import re
import urllib

class resubmit(SubCommand):
    """
    Resubmit the failed jobs of the task identified by the -d/--dir option.
    """
    def __init__(self, logger, cmdargs = None):
        """
        Class constructor.
        """
        self.jobids        = None
        self.sitewhitelist = None
        self.siteblacklist = None
        self.maxjobruntime = None
        self.maxmemory     = None
        self.numcores      = None
        self.priority      = None

        SubCommand.__init__(self, logger, cmdargs)


    def __call__(self):

        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version = __version__)

        msg = "Requesting resubmission for failed jobs in task %s" % (self.cachedinfo['RequestName'])
        self.logger.debug(msg)

        configreq = {'workflow': self.cachedinfo['RequestName']}
        for attr_name in ['jobids', 'sitewhitelist', 'siteblacklist', 'maxjobruntime', 'maxmemory', 'numcores', 'priority']:
            attr_value = getattr(self, attr_name)
            if attr_value:
                configreq[attr_name] = attr_value

        self.logger.info("Sending the request to the server")
        self.logger.debug("Submitting %s " % str(configreq))
        ## TODO: this shouldn't be hard-coded.
        listParams = ['jobids', 'sitewhitelist', 'siteblacklist']
        configreq_encoded = self._encodeRequest(configreq, listParams)
        self.logger.debug("Encoded resubmit request: %s" % (configreq_encoded))

        dictresult, status, reason = server.post(self.uri, data = configreq_encoded)
        self.logger.debug("Result: %s" % (dictresult))
        if status != 200:
            msg = "Problem resubmitting the task to the server:\ninput:%s\noutput:%s\nreason:%s" \
                  % (str(data), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)
        msg = "Resubmit request successfuly sent to the CRAB3 server."
        if dictresult['result'][0]['result'] != 'ok':
            msg += "\nServer responded with: '%s'" % (dictresult['result'][0]['result'])
            returndict = {'status': 'FAILED'}
        else:
            returndict = {'status': 'SUCCESS'}
        self.logger.info(msg)

        return returndict


    ## TODO: This method is shared with submit. Put it in a common place.
    def _encodeRequest(self, configreq, listParams):
        """
        Similar method as in submit.
        """
        encodedLists = ''
        for lparam in listParams:
            if lparam in configreq:
                if len(configreq[lparam]) > 0:
                    encodedLists += ('&%s=' % lparam) + ('&%s=' % lparam).join(map(urllib.quote, configreq[lparam]))
                del configreq[lparam]
        encoded = urllib.urlencode(configreq) + encodedLists
        return str(encoded)


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options.
        """
        self.parser.add_option('--jobids',
                               dest = 'jobids',
                               default = None,
                               help = "The ids of the jobs to resubmit. Comma separated list of integers.",
                               metavar = 'JOBIDS')

        self.parser.add_option('--whitelist',
                               dest = 'sitewhitelist',
                               default = None,
                               help = "Set the sites you want to whitelist for the resubmission." + \
                                      " Comma separated list of CMS site names (e.g.: T2_ES_CIEMAT,T2_IT_Rome[...]).")

        self.parser.add_option('--blacklist',
                               dest = 'siteblacklist',
                               default = None,
                               help = "Set the sites you want to blacklist for the resubmission." + \
                                      " Comma separated list of CMS site names (e.g.: T2_ES_CIEMAT,T2_IT_Rome[...]).")

        self.parser.add_option('--walltime',
                               dest = 'maxjobruntime',
                               default = None,
                               type = 'int',
                               help = "Set the maximum time (in minutes) jobs in this task are allowed to run." + \
                                      " Default is 1315 (21 hours 50 minutes).")

        self.parser.add_option('--memory',
                               dest = 'maxmemory',
                               default = None,
                               type = 'int',
                               help = "Set the maximum memory (in MB) used per job in this task." + \
                                      " Default is 2000.")

        self.parser.add_option('--cores',
                               dest = 'numcores',
                               default = None,
                               type = 'int',
                               help = "Set the number of cores used per job in this task." + \
                                      " (e.g.: 1 for single-threaded applications).")

        self.parser.add_option('--priority',
                               dest = 'priority',
                               default = None,
                               type = 'int',
                               help = "Set the priority of this task compared to other tasks you own; tasks default to 10." + \
                                      " This does not improve your share compared to other users.")


    def validateOptions(self):
        """
        Check if the sitelist parameter is a comma separater list of cms sitenames,
        and put the strings to be passed to the server to self
        """
        SubCommand.validateOptions(self)

        ## Check the format of the jobids option.
        if getattr(self.options, 'jobids'):
            jobidstuple = validateJobids(self.options.jobids)
            self.jobids = [str(jobid) for (_, jobid) in jobidstuple]

        #Checking if the sites provided by the user are valid cmsnames. Doing this because with only the
        #server error handling we get:
        #    Server answered with: Invalid input parameter
        #    Reason is: Incorrect 'siteblacklist' parameter
        #which is not really user friendly.
        #Moreover, I prefer to be independent from Lexicon. I'll the regex here.
        sn_re = "^T[1-3]_[A-Z]{2}(_[A-Za-z0-9]+)+$" #sn_re => SiteName_RegularExpression
        sn_rec = re.compile(sn_re) #sn_rec => SiteName_RegularExpressionCompiled
        for sitelist in ['sitewhitelist', 'siteblacklist']:
            if getattr(self.options, sitelist) is not None:
                for i, site_name in enumerate(getattr(self.options, sitelist).split(',')):
                    if not sn_rec.match(site_name):
                        msg  = "The site name %s does not look like a valid CMS site name" % (site_name)
                        msg += " (it is not matching the regular expression %s)." % (sn_re)
                        raise ConfigurationException(msg)
                setattr(self, sitelist, getattr(self.options, sitelist).split(','))

        ## Sanity checks for task sizes. Limits are purposely fairly generous to provide
        ## some level of future-proofing. The server may restrict further.
        if self.options.maxjobruntime is not None:
            if self.options.maxjobruntime < 60 or self.options.maxjobruntime > 336*60:
                msg = "The requested maximum job runtime (%d minutes) must be between 60 and 20160 minutes." % (self.options.maxjobruntime)
                raise ConfigurationException(msg)
            self.maxjobruntime = str(self.options.maxjobruntime)

        if self.options.maxmemory is not None:
            if self.options.maxmemory < 30 or self.options.maxmemory > 1024*30:
                msg = "The requested per-job memory (%d MB) must be between 30 and 30720 MB." % (self.options.maxmemory)
                raise ConfigurationException(msg)
            self.maxmemory = str(self.options.maxmemory)

        if self.options.numcores is not None:
            if self.options.numcores < 1 or self.options.numcores > 128:
                msg = "The requested number of cores (%d) must be between 1 and 128." % (self.options.numcores)
                raise ConfigurationException(msg)
            self.numcores = str(self.options.numcores)

        if self.options.priority is not None:
            self.priority = str(self.options.priority)

