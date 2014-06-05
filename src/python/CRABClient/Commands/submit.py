"""
This is simply taking care of job submission
"""
import json, os
from string import upper
import types
import imp
import urllib
import time

from RESTInteractions import HTTPRequests

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient import SpellChecker
from CRABClient.client_exceptions import MissingOptionException, ConfigurationException, RESTCommunicationException
from CRABClient.client_utilities import getJobTypes, createCache, addPlugin, server_info, colors
from CRABClient import __version__


class submit(SubCommand):
    """ Perform the submission to the CRABServer
    """

    shortnames = ['sub']

    def __call__(self):
        valid = False
        configmsg = 'Default'

        if not os.path.isfile(self.options.config):
            raise MissingOptionException("Configuration file '%s' not found" % self.options.config)

        self.logger.debug("Started submission")
        # Get some debug parameters
        oneEventMode = hasattr(self.configuration, 'Debug') and \
                                getattr(self.configuration.Debug, 'oneEventMode')
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

        uniquerequestname = None

        self.logger.debug("Working on %s" % str(self.requestarea))

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
                if temp is not None:
                    if mustbetype == type(temp):
                        configreq[param] = temp
                    else:
                        raise ConfigurationException("Invalid type " + str(type(temp)) + " for parameter " + self.requestmapper[param]['config'] \
                                   + ". It is needed a " + str(mustbetype) + ".")
                elif self.requestmapper[param]['default'] is not None:
                    configreq[param] = self.requestmapper[param]['default']
                    temp = self.requestmapper[param]['default']
                elif self.requestmapper[param]['required']:
                    raise ConfigurationException("Missing parameter " + self.requestmapper[param]['config'] + " from the configuration.")
                else:
                    ## parameter not strictly required
                    pass
            if param == "workflow":
                if mustbetype == type(self.requestname):
                    configreq["workflow"] = self.requestname
            elif param == "savelogsflag":#TODO use clientmappig to do this
                configreq["savelogsflag"] = 1 if temp else 0
            elif param == "publication":
                configreq["publication"] = 1 if temp else 0
            elif param == "nonprodsw":
                configreq["nonprodsw"] = 1 if temp else 0
            elif param == "ignorelocality":
                configreq["ignorelocality"] = 1 if temp else 0
            elif param == 'saveoutput':
                configreq['saveoutput'] = 1 if temp else 0
        if (configreq['saveoutput'] or configreq['savelogsflag']) and 'asyncdest' not in configreq:
            raise ConfigurationException("Missing parameter " + self.requestmapper['asyncdest']['config'] + " from the configuration.")

        # Add debug parameters to the configreq dict
        configreq['oneEventMode'] = int(oneEventMode)

        jobconfig = {}
        self.configuration.JobType.proxyfilename = self.proxyfilename
        self.configuration.JobType.capath = HTTPRequests.getCACertPath()
        #get the backend URLs from the server external configuration
        serverBackendURLs = server_info('backendurls', self.serverurl, self.proxyfilename, self.getUrl(self.instance, resource='info'))
        #if cacheSSL is specified in the server external configuration we will use it to upload the sandbox (baseURL will be ignored)
        self.configuration.JobType.filecacheurl = serverBackendURLs['cacheSSL'] if 'cacheSSL' in serverBackendURLs else None
        pluginParams = [ self.configuration, self.logger, os.path.join(self.requestarea, 'inputs') ]
        if getattr(self.configuration.JobType, 'pluginName', None) is not None:
            jobtypes    = getJobTypes()
            plugjobtype = jobtypes[upper(self.configuration.JobType.pluginName)](*pluginParams)
            inputfiles, jobconfig, isbchecksum = plugjobtype.run(configreq)
        else:
            fullname = self.configuration.JobType.externalPluginFile
            basename = os.path.basename(fullname).split('.')[0]
            plugin = addPlugin(fullname)[basename]
            pluginInst = plugin(*pluginParams)
            inputfiles, jobconfig, isbchecksum = pluginInst.run(configreq)

        if not configreq['publishname']:
            configreq['publishname'] =  isbchecksum
        else:
            configreq['publishname'] = "%s-%s" %(configreq['publishname'], isbchecksum)
        configreq.update(jobconfig)

        server = HTTPRequests(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

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
        createCache(self.requestarea, tmpsplit[0], tmpsplit[1] if len(tmpsplit)>1 else '', uniquerequestname,
                    voRole=self.voRole, voGroup=self.voGroup, instance=self.instance,
                    originalConfig = self.configuration)

        self.logger.info(colors.GREEN+"Your task has been delivered to the CRAB3 server."+colors.NORMAL)
        if not self.options.wait:
            self.logger.info("Please use 'crab status' to check how the submission process proceed")
            self.logger.debug("Request ID: %s " % uniquerequestname)

        if self.options.wait:
            self.checkStatusLoop(server,uniquerequestname)

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

        self.parser.add_option( "-w","--wait,",
                                action="store_true",
                                dest="wait",
                                help="continuously checking for job status after submitting",
                                default=False )


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
        elif getattr(self.configuration.Site, "storageSite", None) is None and\
             (getattr(self.configuration.General, 'transferOutput', True) or getattr(self.configuration.General, 'saveLogs', False)):
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
        listParams = ['adduserfiles', 'addoutputfiles', 'sitewhitelist', 'siteblacklist', 'blockwhitelist', 'blockblacklist',
                      'tfileoutfiles', 'edmoutfiles', 'runs', 'lumis', 'userfiles']
        encodedLists = ''
        for lparam in listParams:
            if lparam in configreq:
                if len(configreq[lparam])>0:
                    encodedLists += ('&%s=' % lparam) + ('&%s=' % lparam).join( map(urllib.quote, configreq[lparam]) )
                del configreq[lparam]
        encoded = urllib.urlencode(configreq) + encodedLists
        self.logger.debug('Encoded submit request: %s' % encoded)
        return encoded

    def checkStatusLoop(self,server,uniquerequestname):

        self.logger.info("Waiting for task to be processed")

        maxwaittime= 900 #in second, changed to 15 minute max wait time, the original 1 hour is too long 
        starttime=currenttime=time.time()
        endtime=currenttime+maxwaittime

        startimestring=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(starttime))
        endtimestring=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(endtime))

        self.logger.debug("Start time:%s" % startimestring )
        self.logger.debug("Max wait time: %s s until : %s" % (maxwaittime,endtimestring))

        #self.logger.debug('Looking up detailed status of task %s' % uniquerequestname)

        continuecheck=True
        tmpresult=None
        self.logger.info("Checking task status")

        while continuecheck:
            currenttime=time.time()
            querytimestring=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(currenttime))

            self.logger.debug('Looking up detailed status of task %s' % uniquerequestname)

            dictresult, status, reason = server.get(self.uri, data = { 'workflow' : uniquerequestname})
            dictresult = dictresult['result'][0]

            if status != 200:
                self.logger.info("The task has been submitted, \nImpossible to check task status now. \nPlease check again later by using: crab status -t  <Task Name>")
                msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(uniquerequestname), str(dictresult), str(reason))
                raise RESTCommunicationException(msg)

            self.logger.debug("Query Time:%s Task status:%s" %(querytimestring, dictresult['status']))

            if  dictresult['status'] != tmpresult:
                self.logger.info("Task status:%s" % dictresult['status'])
                tmpresult = dictresult['status']

                if dictresult['status'] == 'FAILED':
                    continuecheck = False
                    self.logger.info(self.logger.info(colors.RED+"The submission of your task failed. Please use 'crab status -t <Task Name>' to get the error message" + colors.NORMAL))
                elif dictresult['status'] == 'SUBMITTED' or dictresult['status'] == 'UNKNOWN': #untile the node_state file is available status is unknown
                    continuecheck = False
                    self.logger.info(colors.GREEN+"Your task has been processed and your jobs have been submitted successfully"+colors.NORMAL)
                elif dictresult['status'] in ['NEW','HOLDING','QUEUED']:
                    self.logger.info("Please wait...")
                    time.sleep(30) #the original 60 second query time is too long
                else:
                    continuecheck = False
                    self.logger.info("Please check crab.log ")
                    self.logger.debug("CRABS Status other than FAILED,SUBMITTED,NEW,HOLDING,QUEUED")


            if currenttime > endtime:
                continuecheck = False
                self.logger.info("Exceed max query time \n Please check again later by using: crab status -t  <Task Name>")
                waittime=currenttime-starttime
                self.logger.debug("Wait time:%s" % waittime)
                break

        self.logger.debug("Ended submission process")

