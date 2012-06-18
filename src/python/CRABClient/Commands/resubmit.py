from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import ConfigException, RESTCommunicationException
import urllib
import re

class resubmit(SubCommand):
    """ Resubmit the failed jobs of the task identified by
    -t/--task option
    """
    def __call__(self):
        ## retrieving output files location from the server
        server = HTTPRequests(self.serverurl, self.proxyfilename)

        self.logger.debug('Requesting resubmission for failed jobs in task %s' % self.cachedinfo['RequestName'] )
        #inputdict = { "TaskResubmit": "Analysis", "ForceResubmit" : force }
        dictresult, status, reason = server.post(self.uri, data = urllib.urlencode({ 'workflow' : self.cachedinfo['RequestName']}) + \
                                                    self.sitewhitelist + self.siteblacklist)
        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving resubmitting the task to the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputdict), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        self.logger.info("Resubmission succesfully requested")

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-b", "--blacklist",
                                 dest = 'siteblacklist',
                                 default = None,
                                 help = "Set the sites you want to blacklist during the resubmission." + \
                                            "Comma separated list of cms sites (e.g.: T2_ES_CIEMAT,T2_IT_Rome[...])")

        self.parser.add_option( "-w", "--whitelist",
                                 dest = 'sitewhitelist',
                                 default = None,
                                 help = "Set the sites you want to whitelist during the resubmission." + \
                                            "Comma separated list of cms sites (e.g.: T2_ES_CIEMAT,T2_IT_Rome[...])")

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
        sn_rec = re.compile("^T[1-3]_[A-Z]{2}(_[A-Za-z0-9]+)+$") #sn_rec => SiteName_RegularExpressionCompiled

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
            print self.options, siteList, result
            setattr(self, siteList, result)

