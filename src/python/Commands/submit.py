"""
This is simply taking care of job submission
"""

from CredentialInteractions import CredentialInteractions
from Commands import CommandResult
from client_utilities import getJobTypes, createCache, createWorkArea
import json, os, time
from string import upper
from Commands.SubCommand import SubCommand
from Commands.server_info import server_info
from Commands.reg_user import reg_user
from WMCore.Configuration import loadConfigurationFile, Configuration
from ServerInteractions import HTTPRequests


class submit(SubCommand):
    """
    Perform the submission to the CRABServer
    """

    ## name should become automatically generated
    name  = "submit"
    usage = "usage: %prog " + name + " [options] [args]"


    def loadConfig(self, config):
        """
        Load the configuration file
        """
        self.logger.debug('Loading configuration')
        if type(config) == Configuration:
            self.configuration = config
        else:
            self.configuration = loadConfigurationFile(config)


    def __call__(self, args):

        (options, args) = self.parser.parse_args( args )
        try:
            self.loadConfig( os.path.abspath(options.config) )
        except ImportError:
            return CommandResult(1, "Configuration file '%s' not found" % options.config)

        if getattr(self.configuration, 'General', None) is None:
            return CommandResult(1, "Error: General section is missing in the configuration file.")

        requestarea, requestname = createWorkArea( self.logger,
                                                   getattr(self.configuration.General, 'workArea', None),
                                                   getattr(self.configuration.General, 'requestName', None)
                                                 )

        self.logger.debug("Started submission")

        infosubcmd = server_info(self.logger)
        (code, serverinfo) = infosubcmd(['-s', self.configuration.General.server_url])

        print serverinfo
        # If I'm submitting I need to deal with proxies
        proxy = CredentialInteractions(
                                        serverinfo['server_dn'],
                                        serverinfo['my_proxy'],
                                        getattr(self.configuration.User, "vorole", ""),
                                        getattr(self.configuration.User, "vogroup", ""),
                                        self.logger
                                      )

        self.logger.info("Checking credentials")
        userdn = proxy.createNewVomsProxy( timeleftthreshold = 600 )
        self.logger.info("Registering user credentials")
        proxy.createNewMyProxy( timeleftthreshold = 60 * 60 * 24 * 3)

        uniquerequestname = None

        regusercmd = reg_user(self.logger)
        (code, userinfo) = regusercmd( [
                                        '-s', self.configuration.General.server_url,
                                        '-g', getattr(self.configuration.User, "group", "Analysis"),
                                        '-t', getattr(self.configuration.User, "team", "Analysis"),
                                        '-m', self.configuration.User.email,
                                        '-c', userdn
                                       ])


        self.logger.debug("Working on %s" % str(requestarea))

        copyKeys = ["Group", "Team", "Username", "PublishDataName", "ProcessingVersion", "SaveLogs", ]
        configreq = {
                     "RequestType" : "Analysis",
                     "Group"       : getattr(self.configuration.User, "group", "Analysis"),
                     "Team"        : getattr(self.configuration.User, "team", "Analysis"),
                     "Requestor"   : userinfo['hn_name'],
                     "Username"    : userinfo['hn_name'],
                     "RequestName" : requestname,
                     "RequestorDN" : userdn,
                     "SaveLogs"    : getattr(self.configuration.General, "saveLogs", False),
                     "PublishDataName"   : getattr(self.configuration.Data, "publishDataName", str(time.time())),
                     "ProcessingVersion" : getattr(self.configuration.Data, "processingVersion", 'v1')
                    }

        if getattr(self.configuration, 'Site', None) is not None:
            if len( getattr(self.configuration.Site, "whitelist", []) ) > 0 and self.configuration.Site.whitelist is list :
                configreq["SiteWhitelist"] = self.configuration.Site.whitelist

            if len( getattr(self.configuration.Site, "blacklist", []) ) > 0 and self.configuration.Site.blacklist is list :
                configreq["SiteBlacklist"] = self.configuration.Site.blacklist
   
        if len( getattr(self.configuration.Data, "runWhitelist", []) ) > 0 and self.configuration.Data.runWhitelist is list :
            configreq["RunWhitelist"] = self.configuration.Data.runWhitelist

        if len( getattr(self.configuration.Data, "runBlacklist", []) ) > 0 and self.configuration.Data.runBlacklist is list :
            configreq["RunBlacklist"] = self.configuration.Data.runBlacklist

        if len( getattr(self.configuration.Data, "blockWhitelist", []) ) > 0 and self.configuration.Data.blockWhitelist is list :
            configreq["BlockWhitelist"] = self.configuration.Data.blockWhitelist

        if len( getattr(self.configuration.Data, "blockBlacklist", []) ) > 0 and self.configuration.Data.blockBlacklist is list :
            configreq["BlockBlacklist"] = self.configuration.Data.blockBlacklist

        configreq["DbsUrl"] = getattr(self.configuration.Data, "dbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
  
        if getattr(self.configuration.Data, "splitting", None) is not None:
            configreq["JobSplitAlgo"] = self.configuration.Data.splitting


        filesJob = getattr( self.configuration.Data, "filesPerJob", None)
        eventsJob = getattr( self.configuration.Data, "eventsPerJob", None)

        #if both arguments are specified return an error
        if (filesJob is not None) and (eventsJob is not None):
            msg = "You cannot specify both filesPerJob and eventsPerJob parameters. Please choose one."
            self.logger.error(msg)
            return CommandResult(1, msg)

        if filesJob is not None:
            configreq["JobSplitArgs"] = {"files_per_job" : filesJob}

        if eventsJob is not None:
            configreq["JobSplitArgs"] = {"events_per_job" : eventsJob}    

        #AsyncStageOut parameter
        if getattr(self.configuration.User, "storageSite", None):
            configreq["asyncDest"] = self.configuration.User.storageSite

        ## create job types
        jobtypes = getJobTypes()
        if getattr(self.configuration, 'JobType', None) is None:
           return CommandResult(1, "Error: JobType section is missing in the configuration file.")
        if upper(self.configuration.JobType.pluginName) not in jobtypes:
           raise CommandResult(1, "JobType %s not found or not supported." % self.configuration.JobType.pluginName)
        plugjobtype = jobtypes[upper(self.configuration.JobType.pluginName)](self.configuration, self.logger, os.path.join(requestarea, 'inputs'))
        inputfiles, jobconfig = plugjobtype.run(configreq)

        configreq.update(jobconfig)

        ## TODO upload inputfiles
        #self.logger.debug("Uploading inputfiles '%s' should be here" % str(inputfiles))
        #    self.configuration.General.sbservhost
        #    self.configuration.General.sbservport
        #    self.configuration.General.sbservtype
        #    self.configuration.General.sbservpath

        server = HTTPRequests(self.configuration.General.server_url)

        uri = '/crabinterface/crab/task/' + configreq["RequestName"]

        self.logger.info("Sending the request to the server")
        self.logger.debug("Submitting %s " % str( json.dumps( configreq, sort_keys = False, indent = 4 ) ) )
        dictresult, status, reason = server.post(uri, json.dumps( configreq, sort_keys = False) )
        self.logger.debug("Result: %s" % dictresult)
        if status != 200:
           msg = "Problem sending the request:\ninput:%s\noutput:%s\nreason:%s" % (str(configreq), str(dictresult), str(reason))
           return CommandResult(1, msg)
        elif dictresult.has_key("ID"):
            uniquerequestname = dictresult["ID"]
        else:
            msg = "Problem during submission, no request ID returned:\ninput:%s\noutput:%s\nreason:%s" \
                   % (str(configreq), str(dictresult), str(reason))
            return CommandResult(1, msg)

        createCache( requestarea, server, uniquerequestname )

        self.logger.info("Submission completed")
        self.logger.debug("Request ID: %s " % uniquerequestname)

        self.logger.debug("Ended submission")

        return CommandResult(0, None)

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """

        self.parser.add_option( "-c", "--config",
                                 dest = "config",
                                 default = './crabConfig.py',
                                 help = "CRAB configuration file",
                                 metavar = "FILE" )

