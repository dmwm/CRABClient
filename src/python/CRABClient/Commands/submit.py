"""
This is simply taking care of job submission
"""

from CRABClient.client_utilities import getJobTypes, createCache, createWorkArea, validServerURL, addPlugin
import json, os
from string import upper
from CRABClient.Commands.SubCommand import SubCommand, ConfigCommand
from CRABClient.Commands.reg_user import reg_user
from WMCore.Configuration import loadConfigurationFile, Configuration
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient import SpellChecker
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import MissingOptionException, ConfigurationException, RESTCommunicationException
import types
import imp
import urllib

class submit(SubCommand, ConfigCommand):
    """ Perform the submission to the CRABServer
    """

    shortnames = ['sub']

    def __call__(self):
        valid = False
        configmsg = 'Default'

        if not os.path.isfile(self.options.config):
            raise MissingOptionException("Configuration file '%s' not found" % self.options.config)

        #store the configuration file in self.configuration
        self.loadConfig( self.options.config, self.args )

        requestarea, requestname, self.logfile = createWorkArea( self.logger,
                                                                 getattr(self.configuration.General, 'workArea', None),
                                                                 getattr(self.configuration.General, 'requestName', None)
                                                               )

        self.logger.debug("Started submission")

        #determine the serverurl
        if self.options.server:
            self.serverurl = self.options.server
        elif getattr( self.configuration.General, 'serverUrl', None ) is not None:
            self.serverurl = self.configuration.General.serverUrl
#TODO: For sure the server url should not be handled here. Find an intelligent way for this
        else:
            self.serverurl = 'http://cmsweb.cern.ch'
        if not hasattr( self.configuration.General, 'ufccacheUrl' ):
            self.configuration.General.ufccacheUrl = self.serverurl
        if not hasattr( self.configuration.General, 'configcacheUrl' ):
            #https is required because configcache does not use ServerInteractions
            self.configuration.General.configcacheUrl = 'https://' + self.serverurl + '/couchdb'
        if not hasattr( self.configuration.General, 'configcacheName' ):
            self.configuration.General.configcacheName = 'analysis_reqmgr_config_cache'

        self.createCache( self.serverurl )

        ######### Check if the user provided unexpected parameters ########
        #init the dictionary with all the known parameters
        SpellChecker.DICTIONARY = SpellChecker.train( [ val['config'] for _, val in self.requestmapper.iteritems() if val['config'] ] + \
                                                      [ x for x in self.otherConfigParams ] )
        #iterate on the parameters provided by the user
        for section in self.configuration.listSections_():
            for attr in getattr(self.configuration, section).listSections_():
                par = (section + '.' + attr)
                #if the parameter is not know exit, but try to correct it before
                if not SpellChecker.is_correct( par ):
                    msg = 'The parameter %s is not known.' % par
                    msg += '' if SpellChecker.correct(par) == par else ' Did you mean %s?' % SpellChecker.correct(par)
                    raise ConfigurationException(msg)

        #usertarball and cmsswconfig use this parameter and we should set it up in a correct way
        self.configuration.General.serverUrl = self.serverurl

        #delegating the proxy (creation done in SubCommand)
        self.voRole = getattr(self.configuration.User, "voRole", "")
        self.voGroup = getattr(self.configuration.User, "voGroup", "")
        self.handleProxy()

        uniquerequestname = None

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
                        raise ConfigurationException(1, "Invalid type " + str(type(temp)) + " for parameter " + self.requestmapper[param]['config'] \
                                   + ". It is needed a " + str(mustbetype) + ".")
                elif self.requestmapper[param]['default'] is not None:
                    configreq[param] = self.requestmapper[param]['default']
                elif self.requestmapper[param]['required']:
                    raise ConfigurationException(1, "Missing parameter " + self.requestmapper[param]['config'] + " from the configuration.")
                else:
                    ## parameter not strictly required
                    pass
            if param == "workflow":
                if mustbetype == type(requestname):
                    configreq["workflow"] = requestname
            elif param == "savelogsflag":
                configreq["savelogsflag"] = 1 if temp else 0
            elif param == "blacklistT1":
                blacklistT1 = self.voRole != 't1access'
                #if the user choose to remove the automatic T1 blacklisting and has not the t1acces role
                if getattr (self.configuration.Site, 'removeT1Blacklisting', False) and blacklistT1:
                    self.logger.info("WARNING: You disabled the T1 automatic blacklisting without having the t1access role")
                    blacklistT1 = False
                configreq["blacklistT1"] = 1 if blacklistT1 else 0
            elif self.requestmapper[param]['required']:
                if self.requestmapper[param]['default'] is not None:
                    configreq[param] = self.requestmapper[param]['default']

        jobconfig = {}
        self.configuration.JobType.proxyfilename = self.proxyfilename
        self.configuration.JobType.capath = HTTPRequests.getCACertPath()
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

        server = HTTPRequests(self.serverurl, self.proxyfilename)

        self.logger.info("Sending the request to the server")
        self.logger.debug("Submitting %s " % str( configreq ) )

        dictresult, status, reason = server.put( self.uri, data = self._encodeRequest(configreq) )
        self.logger.debug("Result: %s" % dictresult)
        if status != 200:
            msg = "Problem sending the request:\ninput:%s\noutput:%s\nreason:%s" % (str(configreq), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)
        elif dictresult.has_key("result"):
            uniquerequestname = dictresult["result"][0]["RequestName"]
        else:
            msg = "Problem during submission, no request ID returned:\ninput:%s\noutput:%s\nreason:%s" \
                   % (str(configreq), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        tmpsplit = self.serverurl.split(':')
        createCache( requestarea, tmpsplit[0], tmpsplit[1] if len(tmpsplit)>1 else '', uniquerequestname, voRole = self.voRole, voGroup = self.voGroup )

        self.logger.info("Submission completed")
        self.logger.debug("Request ID: %s " % uniquerequestname)

        self.logger.debug("Ended submission")

        return uniquerequestname


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

    def _encodeRequest( self, configreq):
        """ Used to encode the request from a dict to a string. Include the code needed for transforming lists in the format required by
            cmsweb, e.g.:   adduserfiles = ['file1','file2']  ===>  [...]adduserfiles=file1&adduserfiles=file2[...]
        """
        #TODO automate this using ClientMapping
        listParams = ['adduserfiles', 'addoutputfiles', 'sitewhitelist', 'siteblacklist', 'blockwhitelist', 'blockblacklist', 'runs', 'lumis']
        encodedLists = ''
        for lparam in listParams:
            if lparam in configreq:
                if len(configreq[lparam])>0:
                    encodedLists += ('&%s=' % lparam) + ('&%s=' % lparam).join( map(urllib.quote, configreq[lparam]) )
                del configreq[lparam]
        encoded = urllib.urlencode(configreq) + encodedLists
        self.logger.debug('Encoded submit request: %s' % encoded)
        return encoded
