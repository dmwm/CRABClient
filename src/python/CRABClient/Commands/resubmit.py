from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import ConfigException, RESTCommunicationException
from WMCore.Credential.Proxy import Proxy
from CRABClient.client_utilities import validateJobids

from urllib import urlencode
import re


class resubmit(SubCommand):
    """ Resubmit the failed jobs of the task identified by
    -t/--task option
    """
    def __call__(self):
        self.logger.debug('Requesting resubmission for failed jobs in task %s' % self.cachedinfo['RequestName'] )
        if not self.standalone:
            self.logger.debug('Requesting remote/server resubmission of jobs')
            ## retrieving output files location from the server
            server = HTTPRequests(self.serverurl, self.proxyfilename)

            #inputdict = { "TaskResubmit": "Analysis", "ForceResubmit" : force }
            dictresult, status, reason = server.post(self.uri, data = urllib.urlencode({ 'workflow' : self.cachedinfo['RequestName']}) + \
                                                        self.sitewhitelist + self.siteblacklist)
            self.logger.debug("Result: %s" % dictresult)

            if status != 200:
                msg = "Problem retrieving resubmitting the task to the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputdict), str(dictresult), str(reason))
                raise RESTCommunicationException(msg)
            if dictresult['result'][0]['result'] != 'ok':
                self.logger.info(dictresult['result'][0]['result'])
        else:
            self.logger.debug('Requesting local htcondor submission for failed jobs')
            proxy = Proxy({'logger': self.logger})
            # TODO: make this into a function since everyone does it
            from CRABInterface.DagmanDataWorkflow import DagmanDataWorkflow
            dag = DagmanDataWorkflow(config = self.cachedinfo['OriginalConfig'])
            status = 200
            reason = 'OK'
            userdn = proxy.getSubject(self.proxyfilename)
            dictresult = dag.resubmit(self.cachedinfo['RequestName'], self.sitewhitelist, self.siteblacklist, userdn=userdn)

        self.logger.info("Resubmit request succesfully sent")
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

        self.parser.add_option( '-i', '--jobids',
                                dest = 'jobids',
                                default = None,
                                help = 'Ids of the jobs you want to resubmit. Comma separated list of intgers',
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
        self.sitewhitelist = ''
        self.siteblsklist = ''

        for siteList in ['sitewhitelist', 'siteblacklist']:
            result = ''
            paramVal = getattr(self.options, siteList, None)
            if paramVal:
                for site in paramVal.split(','):
                    try:
                        WMCore.Lexicon.cmsname(site)
                    except:
                        raise ConfigException("The sitename %s does not look like a valid CMS name" % site )
                    # ... why are we constructing a HTTP query string here?
                    result += "&%s=%s" % (siteList, site)
            setattr(self, siteList, result)

        #check the format of jobids
        self.jobids = ''
        if getattr(self.options, 'jobids', None):
            self.jobids = validateJobids(self.options.jobids)
