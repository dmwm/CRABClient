"""
Module to handle CMSSW _cfg.py file
"""

import imp
import json
import os
import sys
import hashlib
import logging

from CRABClient.ServerInteractions import HTTPRequests
from PSetTweaks.WMTweak import makeTweak
from CRABClient.client_exceptions import ConfigException
from WMCore.Cache.WMConfigCache import ConfigCache, ConfigCacheException

class CMSSWConfig(object):
    """
    Class to handle CMSSW _cfg.py file
    """
    def __init__(self, config, userConfig=None, logger=None):
        self.config = config
        self.logger = logger

        self.fullConfig = None
        self.outputFile = None
        #TODO: Deal with user parameters (pycfg_params)

        if userConfig:
            cfgBaseName = os.path.basename(userConfig).replace(".py", "")
            cfgDirName = os.path.dirname(userConfig)

            if not os.path.isfile( userConfig ):
                msg = "Cannot find file %s" % userConfig
                raise ConfigException( msg )

            self.logger.debug("Importing CMSSW config %s" % userConfig)
            modPath = imp.find_module(cfgBaseName, [cfgDirName])

            pyCfgParams = getattr(self.config.JobType, 'pyCfgParams', [])
            if pyCfgParams:
                originalArgv = sys.argv
                sys.argv = [userConfig]
                sys.argv.extend(pyCfgParams)

            self.fullConfig = imp.load_module(cfgBaseName, modPath[0],
                                              modPath[1],  modPath[2])
            if pyCfgParams: # Restore original sys.argv
                sys.argv = originalArgv

            self.tweakJson = makeTweak(self.fullConfig.process).jsonise()


    #TODO Since we are able to write direcly in couch, how can I guarantee a user does not delete other user's config caches?
    def upload(self, requestConfig):
        """
        Upload the config file to the server
        """
        if not self.outputFile:
            raise ConfigException('You must write out the config before uploading it')

        with open(self.outputFile) as cfgFile:
            configString = cfgFile.read()

        try:
            configCache = ConfigCache(self.config.General.configcacheUrl, self.config.General.configcacheName, usePYCurl=True, \
                                      ckey=self.config.JobType.proxyfilename, cert=self.config.JobType.proxyfilename, \
                                      capath=self.config.JobType.capath)
            configCache.createUserGroup("Analysis", '') #User empty works

            configMD5 = hashlib.md5(configString).hexdigest()
            configCache.document['md5_hash'] = configMD5
            configCache.document['pset_hash'] = '' #Why that was empty?
            configCache.attachments['configFile'] = configString

            configCache.setPSetTweaks(json.loads(self.tweakJson))

            configCache.setLabel('')
            configCache.setDescription('')
            configCache.save()
            result = {}
            result['DocID']  = configCache.document["_id"]
            result['DocRev'] = configCache.document["_rev"]
        except ConfigCacheException, ex:
            msg = "Error: problem uploading the configuration"
            logging.getLogger('CRAB3:traceback').exception('Caught exception')
            raise ConfigException("Problem during the upload of the configuration.")
        return result


    def writeFile(self, filename= 'CMSSW.py'):
        """
        Persist fully expanded _cfg.py file
        """

        self.outputFile = filename
        self.logger.debug("Writing CMSSW config to %s" % self.outputFile)
        outFile = open(filename, "wb")
        outFile.write("import FWCore.ParameterSet.Config as cms\n")
        outFile.write(self.fullConfig.process.dumpPython())
        outFile.close()

        # Would like to store as a single pickle string rather than dumpPython
        #  which has been unreliable at times.
        #outFile.write("import pickle\n")
        #outFile.write('process = pickle.loads("""\n')
        #outFile.write(pickle.dumps(self.fullConfig.process))
        #outFile.write('\n"""')

        return


    def outputFiles(self):
        """
        Returns a tuple of lists of output files. First element is TFileService files,
        second is PoolOutput files
        """

        tFiles = []
        poolFiles = []

        # Find TFileService
        if self.fullConfig.process.services.has_key('TFileService'):
            tFileService = self.fullConfig.process.services['TFileService']
            if "fileName" in tFileService.parameterNames_():
                tFiles.append(getattr(tFileService, 'fileName', None).value())

        # Find files written by output modules
        poolFiles = []
        outputModuleNames = self.fullConfig.process.outputModules_().keys()

        for outputModName in outputModuleNames:
            outputModule = getattr(self.fullConfig.process, outputModName)
            poolFiles.append(outputModule.fileName.value())

        # If there are multiple output files, make sure they have filterNames set
        if len(outputModuleNames) > 1:
            for outputModName in outputModuleNames:
                try:
                    outputModule = getattr(self.fullConfig.process, outputModName)
                    dataset = getattr(outputModule, 'dataset')
                    filterName = getattr(dataset, 'filterName')
                except AttributeError:
                    raise RuntimeError('Your output module %s does not have a "dataset" PSet ' % outputModName +
                                       'or the PSet does not have a "filterName" member.')

        return tFiles, poolFiles
