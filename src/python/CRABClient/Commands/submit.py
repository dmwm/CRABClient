"""
This is simply taking care of job submission
"""

from CRABClient.Commands import CommandResult
from CRABClient.client_utilities import getJobTypes, createCache, createWorkArea, initProxy, validServerURL, addPlugin
import json, os
from string import upper
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.Commands.server_info import server_info
from CRABClient.Commands.reg_user import reg_user
from WMCore.Configuration import loadConfigurationFile, Configuration
from WMCore.Credential.Proxy import CredentialException
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient import SpellChecker
import types
import imp

class submit(SubCommand):
    """ Perform the submission to the CRABServer
    """

    name  = __name__.split('.').pop()
    usage = "usage: %prog " + name + " [options] [args]"
    names = [name, 'sub']

    splitMap = {'LumiBased' : 'lumis_per_job', 'EventBased' : 'events_per_job', 'FileBased' : 'files_per_job'}

    def loadConfig(self, config, overrideargs=None):
        """
        Load the configuration file
        """
        self.logger.debug('Loading configuration')
        if type(config) == Configuration:
            self.configuration = config
        else:
            self.configuration = loadConfigurationFile( os.path.abspath(config))
        if overrideargs:
            for singlearg in overrideargs:
                fullparname, parval = singlearg.split('=')
                # now supporting just one sub params, eg: Data.inputFiles, User.email, ...
                parnames = fullparname.split('.', 1)
                self.configuration.section_(parnames[0])
                setattr(getattr(self.configuration, parnames[0]), parnames[1], parval)
                self.logger.debug('Overriden parameter %s with %s' % (fullparname, parval))
        return self.validateConfig()


    def __call__(self):
        valid = False
        configmsg = 'Default'
        try:
            valid, configmsg = self.loadConfig( self.options.config, self.args )
        except ImportError:
            return CommandResult(1, "Configuration file '%s' not found" % self.options.config)
        except RuntimeError, re:
            return self._extractReason( re )
        else:
            ## file is there, check if it is ok
            if not valid:
                return CommandResult(1, configmsg)

        requestarea, requestname, self.logfile = createWorkArea( self.logger,
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

        ######### Check if the user provided unexpected parameters ########
        #init the dictionary with all the known parameters
        SpellChecker.DICTIONARY = SpellChecker.train( [ val['config'] if val['config'] else '' for _, val in self.requestmapper.iteritems()] + \
                                                      [ x for x in self._cache['submit']['other-config-params'] ] )
        #iterate on the parameters provided by the user
        for section in self.configuration.listSections_():
            for attr in getattr(self.configuration, section).listSections_():
                par = (section + '.' + attr)
                #if the parameter is not know exit, but try to correct it before
                if not SpellChecker.is_correct( par ):
                    msg = 'The parameter %s is not known.' % par
                    msg += '' if SpellChecker.correct(par) == par else 'Did you mean %s?' % SpellChecker.correct(par)
                    return CommandResult(1, msg)

        #usertarball and cmsswconfig use this parameter and we should set it up in a correct way
        self.configuration.General.serverUrl = serverurl

        infosubcmd = server_info(self.logger, cmdargs = ['-s', serverurl])
        (code, serverinfo) = infosubcmd()
        if code is not 0:
            self.logger.debug("ERROR: failure during retrieval of server information. Stopping submission.")
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
                                         '-g', getattr(self.configuration.User, "group", self.requestmapper["Group"]["default"]),
                                         '-t', getattr(self.configuration.User, "team", self.requestmapper["Team"]["default"]),
                                         '-m', getattr(self.configuration.General, "email", ""),
                                         '-c', userdn
                                        ]
                             )
        (code, userinfo) = regusercmd()
        if code is not 0:
            self.logger.debug("ERROR: user registration failed on the server. Stopping submission.")
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

        unitsPerJob = getattr( self.configuration.Data, "unitsPerJob", None)
        splitMethod = getattr( self.configuration.Data, "splitting",   None)

        if unitsPerJob is not None:
            configreq["JobSplitArgs"] = {self.splitMap[splitMethod] : unitsPerJob}

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
        else:
            if hasattr(self.configuration.Data, 'unitsPerJob'):
                #check that it is a valid number
                try:
                    float(self.configuration.Data.unitsPerJob)
                except ValueError:
                    return False, "Crab configuration problem: unitsPerJob must be a valid number, not %s" % self.configuration.Data.unitsPerJob

            if hasattr(self.configuration.Data, 'splitting'):
                if not self.configuration.Data.splitting in self.splitMap.keys():
                    return False, "Crab configuration problem: The splitting algorithm must be one of %s" % ', '.join(self.splitMap.keys())

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


    def _extractReason(self, re):
        """
        Get the reason of the failure without the stacktrace. Put the stacktrace in the crab.log file
        """
        #get only the error wihtout the stacktrace
        filename = os.path.abspath( self.options.config )
        cfgBaseName = os.path.basename( filename ).replace(".py", "")
        cfgDirName = os.path.dirname( filename )
        if  not cfgDirName:
            modPath = imp.find_module(cfgBaseName)
        else:
            modPath = imp.find_module(cfgBaseName, [cfgDirName])
        try:
            modRef = imp.load_module(cfgBaseName, modPath[0],
                                     modPath[1], modPath[2])
        except Exception, ex:
            msg = str(ex)

        #workarea has not been created yet
        with open('crab.log', 'w') as of:
            of.write(str(re))

        return CommandResult(2000, "Configuration error: \n %s.\nSee the crab.log file for more details" % msg)


#    def terminate(self, exitcode):
#        #exitcode 2000 means there was a python syntax error in the configuration.
#        if exitcode != 2000:
#            SubCommand.terminate( self , exitcode )
