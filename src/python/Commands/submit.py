"""
This is simply taking care of job submission
"""

from CredentialInteractions import CredentialInteractions
from Commands import CommandResult
from client_utilities import getJobTypes, createCache, createWorkArea, initProxy
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
    defaultgroup = "Analysis"
    defaultteam  = "Analysis"
    defaulttype  = "Analysis"

    def loadConfig(self, config):
        """
        Load the configuration file
        """
        self.logger.debug('Loading configuration')
        if type(config) == Configuration:
            self.configuration = config
        else:
            self.configuration = loadConfigurationFile(config)
        return self.validateConfig()


    def __call__(self, args):

        (options, args) = self.parser.parse_args( args )
        valid = False
        configmsg = 'Default'
        try:
            valid, configmsg =self.loadConfig( os.path.abspath(options.config) )
        except ImportError:
            return CommandResult(1, "Configuration file '%s' not found" % options.config)
        else:
            ## file is there, check if it is ok
            if not valid:
                return CommandResult(1, configmsg)

        requestarea, requestname = createWorkArea( self.logger,
                                                   getattr(self.configuration.General, 'workArea', None),
                                                   getattr(self.configuration.General, 'requestName', None)
                                                 )

        self.logger.debug("Started submission")

        infosubcmd = server_info(self.logger)
        (code, serverinfo) = infosubcmd(['-s', self.configuration.General.server_url])
        if code is not 0:
            logging.debug("Error retrieving server information. Stopping submission.")
            return CommandResult(code, serverinfo)

        if not options.skipProxy:
            userdn, proxy = initProxy(
                              serverinfo['server_dn'],
                              serverinfo['my_proxy'],
                              getattr(self.configuration.User, "vorole", ""),
                              getattr(self.configuration.User, "vogroup", ""),
                              True,
                              self.logger
                            )
        else:
            userdn = options.skipProxy
            logging.debug('Skipping proxy creation and delegation. Usind %s as userDN' % userdn)

        uniquerequestname = None

        regusercmd = reg_user(self.logger)
        (code, userinfo) = regusercmd( [
                                        '-s', self.configuration.General.server_url,
                                        '-g', getattr(self.configuration.User, "group", self.defaultgroup),
                                        '-t', getattr(self.configuration.User, "team", self.defaultteam),
                                        '-m', getattr(self.configuration.General, "email", ""),
                                        '-c', userdn
                                       ])
        if code is not 0:
            logging.debug("Error registering user on the server. Stopping submission.")
            return CommandResult(code, serverinfo)

        self.logger.debug("Working on %s" % str(requestarea))

        copyKeys = ["Group", "Team", "Username", "PublishDataName", "ProcessingVersion", "SaveLogs", ]
        configreq = {
                     "RequestType" : self.defaulttype,
                     "Group"       : getattr(self.configuration.User, "group", self.defaultgroup),
                     "Team"        : getattr(self.configuration.User, "team", self.defaultteam),
                     "Requestor"   : userinfo['hn_name'],
                     "Username"    : userinfo['hn_name'],
                     "RequestName" : requestname,
                     "RequestorDN" : userdn,
                     "SaveLogs"    : getattr(self.configuration.General, "saveLogs", False),
                     "PublishDataName"   : getattr(self.configuration.Data, "publishDataName", str(time.time())),
                     "ProcessingVersion" : getattr(self.configuration.Data, "processingVersion", 'v1'),
                     "asyncDest":    self.configuration.Site.storageSite
                    }

        if getattr(self.configuration.Site, "whitelist", None):
            configreq["SiteWhitelist"] = self.configuration.Site.whitelist
        if getattr(self.configuration.Site, "blacklist", None):
            configreq["SiteBlacklist"] = self.configuration.Site.blacklist

        if getattr(self.configuration.Data, "runWhitelist", None):
            configreq["RunWhitelist"] = self.configuration.Data.runWhitelist
        if getattr(self.configuration.Data, "runBlacklist", None):
            configreq["RunBlacklist"] = self.configuration.Data.runBlacklist

        if getattr(self.configuration.Data, "blockWhitelist", None):
            configreq["BlockWhitelist"] = self.configuration.Data.blockWhitelist
        if getattr(self.configuration.Data, "blockBlacklist", None):
            configreq["BlockBlacklist"] = self.configuration.Data.blockBlacklist

        configreq["DbsUrl"] = getattr(self.configuration.Data, "dbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")

        if getattr(self.configuration.Data, "splitting", None) is not None:
            configreq["JobSplitAlgo"] = self.configuration.Data.splitting

        filesJob = getattr( self.configuration.Data, "filesPerJob", None)
        eventsJob = getattr( self.configuration.Data, "eventsPerJob", None)
        if filesJob is not None:
            configreq["JobSplitArgs"] = {"files_per_job" : filesJob}
        if eventsJob is not None:
            configreq["JobSplitArgs"] = {"events_per_job" : eventsJob}
        jobtypes    = getJobTypes()
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

        self.logger.info("Sending the request to the server")
        self.logger.debug("Submitting %s " % str( json.dumps( configreq, sort_keys = False, indent = 4 ) ) )
        dictresult, status, reason = server.post(self.uri + configreq["RequestName"], json.dumps( configreq, sort_keys = False) )
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

        self.parser.add_option( "-p", "--skip-proxy",
                                 dest = "skipProxy",
                                 default = False,
                                 help = "Skip Grid proxy creation and myproxy delegation",
                                 metavar = "USERDN" )


    def validateConfig(self):
        """
        __validateConfig__

        Checking if needed input parameters are there
        """

        if getattr(self.configuration, 'General', None) is None:
            return False, "Crab configuration problem: general section is missing. "
        elif getattr(self.configuration.General, "server_url", None) is None:
            msg = "Crab configuration problem: General.server_url parameter is missing. \n" + \
                  "  (this parameter is just temporary and in future implementations wont be required)"
            return False, msg

        if getattr(self.configuration, 'User', None) is None:
            return False, "Crab configuration problem: User section is missing ."

        if getattr(self.configuration, 'Data', None) is None:
            return False, "Crab configuration problem: Data section is missing. "
        elif getattr( self.configuration.Data, "filesPerJob", None) and getattr( self.configuration.Data, "eventsPerJob", None):
            #if both arguments are specified return an error
            msg = "Crab configuration problem: you cannot specify both filesPerJob and eventsPerJob parameters. Please choose just one. "
            return False, msg

        if getattr(self.configuration, 'Site', None) is None:
            return False, "Crab configuration problem: Site section is missing. "
        elif getattr(self.configuration.Site, "storageSite", None) is None:
            return False, "Crab configuration problem: Site.storageSite parameter is missing. "

        if getattr(self.configuration, 'JobType', None) is None:
            return False, "Crab configuration problem: JobType section is missing. "
        elif getattr(self.configuration.JobType, 'pluginName', None) is None:
            return False, "Crab configuration problem: JobType.pluginName parameter is missing. "
        elif upper(self.configuration.JobType.pluginName) not in getJobTypes():
           msg = "JobType %s not found or not supported." % self.configuration.JobType.pluginName
           raise False, msg

        return True, "Valid configuration"
