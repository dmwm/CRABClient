import re
import urllib

## CRAB dependencies.
import CRABClient.Emulator
from CRABClient import __version__
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientUtilities import validateJobids, checkStatusLoop
from CRABClient.ClientExceptions import ConfigurationException, RESTCommunicationException


class resubmitold(SubCommand):
    """
    Resubmit jobs of the task identified by the -d/--dir option.
    """
    def __init__(self, logger, cmdargs = None):
        """
        Class constructor.
        """
        ## These parameters are defined in setOptions(). They correspond to what
        ## the user passed in the corresponding command line options.
        self.jobids        = None
        self.sitewhitelist = None
        self.siteblacklist = None

        SubCommand.__init__(self, logger, cmdargs)


    def __call__(self):

        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version = __version__)

        if self.jobids:
            msg = "Requesting resubmission of jobs %s in task %s" % (self.jobids, self.cachedinfo['RequestName'])
        else:
            msg = "Requesting resubmission of failed jobs in task %s" % (self.cachedinfo['RequestName'])
        self.logger.debug(msg)

        configreq = {'workflow': self.cachedinfo['RequestName'], 'subresource': 'resubmit'}
        for attr_name in ['jobids', 'sitewhitelist', 'siteblacklist']:
            attr_value = getattr(self, attr_name)
            ## For 'jobids', 'sitewhitelist' and 'siteblacklist', attr_value is either a list of strings or None.
            if attr_value is not None:
                configreq[attr_name] = attr_value
        for attr_name in ['maxjobruntime', 'maxmemory', 'numcores', 'priority']:
            attr_value = getattr(self.options, attr_name)
            ## For 'maxjobruntime', 'maxmemory', 'numcores', and 'priority', attr_value is either an integer or None.
            if attr_value is not None:
                configreq[attr_name] = attr_value
        configreq['force'] = 1 if self.options.force else 0
        configreq['publication'] = 1 if self.options.publication else 0

        self.logger.info("Sending resubmit request to the server.")
        self.logger.debug("Submitting %s " % str(configreq))
        configreq_encoded = self._encodeRequest(configreq)
        self.logger.debug("Encoded resubmit request: %s" % (configreq_encoded))

        dictresult, status, reason = server.post(self.uri, data = configreq_encoded)
        self.logger.debug("Result: %s" % (dictresult))
        if status != 200:
            msg = "Problem resubmitting the task to the server:\ninput:%s\noutput:%s\nreason:%s" \
                  % (str(configreq_encoded), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)
        self.logger.info("Resubmit request sent to the server.")
        if dictresult['result'][0]['result'] != 'ok':
            msg = "Server responded with: '%s'" % (dictresult['result'][0]['result'])
            self.logger.info(msg)
            returndict = {'status': 'FAILED'}
        else:
            if not self.options.wait:
                msg  = "Please use 'crab status' to check how the resubmission process proceeds."
                msg += "\nNotice it may take a couple of minutes for the resubmission to get fully processed."
                self.logger.info(msg)
            else:
                targetTaskStatus = 'SUBMITTED'
                checkStatusLoop(self.logger, server, self.uri, self.cachedinfo['RequestName'], targetTaskStatus, self.name)
            returndict = {'status': 'SUCCESS'}

        return returndict


    ## TODO: This method is shared with submit. Put it in a common place.
    def _encodeRequest(self, configreq):
        """
        Similar method as in submit.
        """
        encodedLists = ""
        listParams = [k for k in configreq.keys() if isinstance(configreq[k], list)]
        for lparam in listParams:
            if len(configreq[lparam]) > 0:
                encodedLists += ("&%s=" % lparam) + ("&%s=" % lparam).join(map(urllib.quote, configreq[lparam]))
            elif len(configreq[lparam]) == 0:
                encodedLists += ("&%s=empty" % lparam)
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

        self.parser.add_option('--sitewhitelist',
                               dest = 'sitewhitelist',
                               default = None,
                               help = "Set the sites you want to whitelist for the resubmission." + \
                                      " Comma separated list of CMS site names (e.g.: T2_ES_CIEMAT,T2_IT_Rome[...]).")

        self.parser.add_option('--siteblacklist',
                               dest = 'siteblacklist',
                               default = None,
                               help = "Set the sites you want to blacklist for the resubmission." + \
                                      " Comma separated list of CMS site names (e.g.: T2_ES_CIEMAT,T2_IT_Rome[...]).")

        self.parser.add_option('--maxjobruntime',
                               dest = 'maxjobruntime',
                               default = None,
                               type = 'int',
                               help = "Set the maximum time (in minutes) jobs in this task are allowed to run." + \
                                      " Default is 1315 (21 hours 50 minutes).")

        self.parser.add_option('--maxmemory',
                               dest = 'maxmemory',
                               default = None,
                               type = 'int',
                               help = "Set the maximum memory (in MB) used per job in this task." + \
                                      " Default is 2000.")

        self.parser.add_option('--numcores',
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
                                      " Tasks with higher priority values run first. This does not change your share compared to other users.")

        self.parser.add_option('--wait',
                               dest = 'wait',
                               default = False,
                               action = 'store_true',
                               help = "Continuously check the task status after resubmission.")

        self.parser.add_option('--force',
                               dest = 'force',
                               default = False,
                               action = 'store_true',
                               help = "Force resubmission of successful jobs indicated in --jobids option.")

        self.parser.add_option('--publication',
                               dest = 'publication',
                               default = False,
                               action = 'store_true',
                               help = "Resubmit only the failed publications.")


    def validateOptions(self):
        """
        Check if the sitelist parameter is a comma separater list of cms sitenames,
        and put the strings to be passed to the server to self
        """
        SubCommand.validateOptions(self)

        if self.options.publication:
            if self.options.sitewhitelist is not None or self.options.siteblacklist is not None or \
               self.options.maxjobruntime is not None or self.options.maxmemory is not None or \
               self.options.numcores is not None or self.options.priority is not None:
                msg  = "The options --sitewhitelist, --siteblacklist,"
                msg += " --maxjobruntime, --maxmemory, --numcores and  --priority"
                msg += " can not be specified together with the option --publication."
                msg += " The last option is to only resubmit (failed) publications,"
                msg += " in which case all of the first options make no sense."
                raise ConfigurationException(msg)
            if self.options.jobids:
                msg  = "The option --jobids"
                msg += " can not be specified together with the option --publication."
                msg += " The last option is to only resubmit (failed) publications,"
                msg += " which does not allow yet filtering on job ids (ALL failed publications will be resubmitted)."
                raise ConfigurationException(msg)
            if self.options.force:
                msg  = "The option --force"
                msg += " can not be specified together with the option --publication."
                msg += " The last option is to only resubmit failed publications."
                msg += " Publications in a status other than 'failed' can not be resubmitted."
                raise ConfigurationException(msg)

        ## The --jobids option indicates which jobs have to be resubmitted. If it is not
        ## given, then all jobs in the task that are not running or successfully
        ## completed are resubmitted. If the user provides a list of job ids, then also
        ## successfully completed jobs can be resubmitted.

        ## Check the format of the jobids option.
        if self.options.jobids:
            jobidstuple = validateJobids(self.options.jobids)
            self.jobids = [str(jobid) for (_, jobid) in jobidstuple]

        ## The --force option should not be accepted unless combined with a user-given
        ## list of job ids via --jobids.
        if self.options.force and not self.jobids:
            msg = "Option --force can only be used in combination with option --jobids."
            raise ConfigurationException(msg)

        ## Covention used for the job parameters that the user can set when doing job
        ## resubmission (i.e. siteblacklist, sitewhitelist, maxjobruntime, maxmemory,
        ## numcores and priority):
        ## - If the user doesn't set a parameter we don't pass it to the server and the
        ##   the server copies the original value the parameter had at task submission.
        ##   It copies it from the Task DB. Therefore we need to keep these parameters
        ##   in separate columns of the Task DB containing their original values.
        ## - For the site black- and whitelists, if the user passes an empty string,
        ##   e.g. --siteblacklist='', we pass to the server siteblacklist=empty and the
        ##   server interprets this as and empty list ([]). If the user passes a given
        ##   list of sites, this new list overwrittes the original one.
        ## - The values of the parameters are used only for the resubmitted jobs (for
        ##   their first resubmission and all next automatic resubmissions).

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
                if getattr(self.options, sitelist) != "":
                    for site_name in getattr(self.options, sitelist).split(','):
                        if '*' not in site_name and not sn_rec.match(site_name):
                            msg  = "The site name '%s' does not look like a valid CMS site name" % (site_name)
                            msg += " (it is not matching the regular expression '%s')." % (sn_re)
                            raise ConfigurationException(msg)
                    setattr(self, sitelist, getattr(self.options, sitelist).split(','))
                else:
                    setattr(self, sitelist, [])

        ## Sanity checks for task sizes. Limits are purposely fairly generous to provide
        ## some level of future-proofing. The server may restrict further.
        if self.options.maxjobruntime is not None:
            if self.options.maxjobruntime < 60 or self.options.maxjobruntime > 336*60:
                msg = "The requested maximum job runtime (%d minutes) must be between 60 and 20160 minutes." % (self.options.maxjobruntime)
                raise ConfigurationException(msg)

        if self.options.maxmemory is not None:
            if self.options.maxmemory < 30 or self.options.maxmemory > 1024*30:
                msg = "The requested per-job memory (%d MB) must be between 30 and 30720 MB." % (self.options.maxmemory)
                raise ConfigurationException(msg)

        if self.options.numcores is not None:
            if self.options.numcores < 1 or self.options.numcores > 128:
                msg = "The requested number of cores (%d) must be between 1 and 128." % (self.options.numcores)
                raise ConfigurationException(msg)

        if self.options.priority is not None:
            if self.options.priority < 1:
                msg = "The requested priority (%d) must be greater than 0." % (self.options.priority)
                raise ConfigurationException(msg)

