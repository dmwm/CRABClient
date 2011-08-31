"""
This is simply taking care of job submission
"""

from Commands import CommandResult
from client_utilities import getJobTypes, createCache, createWorkArea, initProxy, validServerURL, addPlugin
import json, os
from string import upper
from Commands.SubCommand import SubCommand
from Commands.server_info import server_info
from Commands.reg_user import reg_user
from WMCore.Configuration import loadConfigurationFile, Configuration
from WMCore.Credential.Proxy import CredentialException
from ServerInteractions import HTTPRequests
import types


class submit(SubCommand):
    """ Perform the submission to the CRABServer
    """

    name  = __name__.split('.').pop()
    usage = "usage: %prog " + name + " [options] [args]"
    names = [name, 'sub']
    defaultgroup = "Analysis"
    defaultteam  = "Analysis"


    def loadConfig(self, config):
        """
        Load the configuration file
        """
        self.logger.debug('Loading configuration')
        if type(config) == Configuration:
            self.configuration = config
        else:
            self.configuration = loadConfigurationFile( os.path.abspath(config))
        return self.validateConfig()


    def __call__(self):
        valid = False
        configmsg = 'Default'
        try:
            valid, configmsg = self.loadConfig( self.options.config )
        except ImportError:
            return CommandResult(1, "Configuration file '%s' not found" % self.options.config)
        else:
            ## file is there, check if it is ok
            if not valid:
                return CommandResult(1, configmsg)

        requestarea, requestname = createWorkArea( self.logger,
                                                   getattr(self.configuration.General, 'workArea', None),
                                                   getattr(self.configuration.General, 'requestName', None)
                                                 )

        self.logger.debug("Started submission")

        #determine the serverurl
        if self.options.server:
            serverurl = self.options.server
        elif getattr( self.configuration.General, 'serverUrl', None ) is not None:
            serverurl = self.configuration.General.serverUrl
        else:
            serverurl = 'http://crabserver.cern.ch:8888'

        self.createCache( serverurl )
        #usertarball and cmsswconfig use this parameter and we should set it up in a correct way
        self.configuration.General.serverUrl = serverurl

        infosubcmd = server_info(self.logger, cmdargs = ['-s', serverurl])
        (code, serverinfo) = infosubcmd()
        if code is not 0:
            self.logger.debug("Error retrieving server information. Stopping submission.")
            return CommandResult(code, serverinfo)

        if not self.options.skipProxy:
            try:
                userdn, proxy = initProxy(
                                  serverinfo['server_dn'],
                                  serverinfo['my_proxy'],
                                  getattr(self.configuration.User, "voRole", ""),
                                  getattr(self.configuration.User, "voGroup", ""),
                                  True,
                                  self.logger
                                )
            except CredentialException, ce:
                msg = "Problem during proxy creation: \n %s " % ce._message
                return CommandResult(1, msg)
        else:
            userdn = self.options.skipProxy
            self.logger.debug('Skipping proxy creation and delegation. Usind %s as userDN' % userdn)

        uniquerequestname = None

        regusercmd = reg_user(self.logger,
                              cmdargs = [
                                         '-s', serverurl,
                                         '-g', getattr(self.configuration.User, "group", self.defaultgroup),
                                         '-t', getattr(self.configuration.User, "team", self.defaultteam),
                                         '-m', getattr(self.configuration.General, "email", ""),
                                         '-c', userdn
                                        ]
                             )
        (code, userinfo) = regusercmd()
        if code is not 0:
            self.logger.debug("Error registering user on the server. Stopping submission.")
            return CommandResult(code, serverinfo)

        self.logger.debug("Working on %s" % str(requestarea))

        configreq = {}

        for param in self.requestmapper:
            mustbetype = getattr(types, self.requestmapper[param]['type'])

            if self.requestmapper[param]['config']:
                attrs = self.requestmapper[param]['config'].split('.')
                temp = self.configuration
                for attr in attrs:
                    temp = getattr(temp, attr, None)
                    if temp is None:
                        break
                if temp:
                    if mustbetype == type(temp):
                        configreq[param] = temp
                    else:
                        return CommandResult(1, "Unvalid type " + str(type(temp)) + " for parameter " + self.requestmapper[param]['config'] + ". It is needed a " + str(mustbetype) + ".")
                elif self.requestmapper[param]['default'] is not None:
                    configreq[param] = self.requestmapper[param]['default']
                elif self.requestmapper[param]['required']:
                    return CommandResult(1, "Missing parameter " + self.requestmapper[param]['config'] + " from the configuration.")
                else:
                    ## parameter not strictly required
                    pass
            elif param == "Requestor":
                if mustbetype == type(userinfo['hn_name']):
                    configreq["Requestor"] = userinfo['hn_name']
            elif param == "Username":
                if mustbetype == type(userinfo['hn_name']):
                    configreq["Username"] = userinfo['hn_name']
            elif param == "RequestName":
                if mustbetype == type(requestname):
                    configreq["RequestName"] = requestname
            elif param == "RequestorDN":
                if mustbetype == type(userdn):
                    configreq["RequestorDN"] = userdn
            elif self.requestmapper[param]['required']:
                if self.requestmapper[param]['default'] is not None:
                    configreq[param] = self.requestmapper[param]['default']

        filesJob = getattr( self.configuration.Data, "filesPerJob", None)
        eventsJob = getattr( self.configuration.Data, "eventsPerJob", None)
        if filesJob is not None:
            configreq["JobSplitArgs"] = {"files_per_job" : filesJob}
        if eventsJob is not None:
            configreq["JobSplitArgs"] = {"events_per_job" : eventsJob}

        configreq["VoRole"] = getattr( self.configuration.User, "vorole", '' )
        configreq["VoGroup"] = getattr( self.configuration.User, "vogroup", '' )

        jobconfig = {}
        pluginParams = [ self.configuration, self.logger, os.path.join(requestarea, 'inputs') ]
        if getattr(self.configuration.JobType, 'pluginName', None) is not None:
            jobtypes    = getJobTypes()
            plugjobtype = jobtypes[upper(self.configuration.JobType.pluginName)](*pluginParams)
            inputfiles, jobconfig = plugjobtype.run(configreq)
        else:
            fullname = self.configuration.JobType.externalPluginFile
            basename = os.path.basename(fullname).split('.')[0]
            plugin = addPlugin(fullname)[basename]
            pluginInst = plugin(*pluginParams)
            inputfiles, jobconfig = pluginInst.run(configreq)

        configreq.update(jobconfig)

        server = HTTPRequests(serverurl)

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

        self.parser.add_option( "-s", "--server",
                                 dest = "server",
                                 action = "callback",
                                 type   = 'str',
                                 nargs  = 1,
                                 callback = validServerURL,
                                 metavar = "http://HOSTNAME:PORT",
                                 help = "Endpoint server url to use" )


    def validateConfig(self):
        """
        __validateConfig__

        Checking if needed input parameters are there
        """

        if getattr(self.configuration, 'General', None) is None:
            return False, "Crab configuration problem: general section is missing. "

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
        else:
            if getattr(self.configuration.JobType, 'pluginName', None) is None and\
               getattr(self.configuration.JobType, 'externalPluginFile', None) is None:
                return False, "Crab configuration problem: one of JobType.pluginName or JobType.externalPlugin parameters is required. "
            if getattr(self.configuration.JobType, 'pluginName', None) is not None and\
               getattr(self.configuration.JobType, 'externalPluginFile', None) is not None:
                return False, "Crab configuration problem: only one between JobType.pluginName or JobType.externalPlugin parameters is required. "

            externalPlugin = getattr(self.configuration.JobType, 'externalPluginFile', None)
            if externalPlugin is not None:
                addPlugin(externalPlugin)
            elif upper(self.configuration.JobType.pluginName) not in getJobTypes():
                msg = "JobType %s not found or not supported." % self.configuration.JobType.pluginName
                return False, msg

        return True, "Valid configuration"
