"""
Module to handle CMSSW _cfg.py file
"""

import imp
import json
import os
import sys
import hashlib
import logging

from PSetTweaks.WMTweak import makeTweak

from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import ConfigException

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
