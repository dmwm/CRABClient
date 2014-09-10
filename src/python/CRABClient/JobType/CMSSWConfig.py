"""
Module to handle CMSSW _cfg.py file
"""

import imp
import json
import os
import sys
import hashlib
import logging
import pickle

from PSetTweaks.WMTweak import makeTweak

from CRABClient.client_exceptions import ConfigException

configurationCache = {}

class CMSSWConfig(object):
    """
    Class to handle CMSSW _cfg.py file
    """
    def __init__(self, config, userConfig=None, logger=None):
        global configurationCache
        self.config = config
        self.logger = logger

        self.fullConfig = None
        self.outputFile = None
        #TODO: Deal with user parameters (pycfg_params)

        if userConfig:
            cfgBaseName = os.path.basename(userConfig).replace(".py", "")
            cfgDirName = os.path.dirname(userConfig)

            if not os.path.isfile( userConfig ):
                msg = "Cannot find file %s in %s" % (userConfig, os.getcwd())
                raise ConfigException( msg )

            self.logger.debug("Importing CMSSW config %s" % userConfig)
            modPath = imp.find_module(cfgBaseName, [cfgDirName])

            pyCfgParams = getattr(self.config.JobType, 'pyCfgParams', [])
            originalArgv = sys.argv
            sys.argv = [userConfig]
            if pyCfgParams:
                sys.argv.extend(pyCfgParams)
                self.logger.debug("Extended parameters are %s" % pyCfgParams)
            cacheLine = (tuple(sys.path), tuple(modPath[1]), tuple(sys.argv))
            if cacheLine in configurationCache:
                self.fullConfig = configurationCache[cacheLine]
            else:
                self.fullConfig = imp.load_module(cfgBaseName, modPath[0],
                                                    modPath[1],  modPath[2])
                configurationCache[cacheLine] = self.fullConfig
            sys.argv = originalArgv

            self.tweakJson = makeTweak(self.fullConfig.process).jsonise()


    def writeFile(self, filename= 'CMSSW.py'):
        """
        Persist fully expanded _cfg.py file
        """

        self.outputFile = filename
        self.logger.debug("Writing CMSSW config to %s" % self.outputFile)

        #saving the process object as a pickle
        pklFileName = os.path.splitext(filename)[0] + ".pkl"
        pklFile = open(pklFileName,"wb")
        pickle.dump(self.fullConfig.process, pklFile)
        pklFile.close()

        #create the auxiliary file
        outFile = open(filename, "wb")
        outFile.write("import FWCore.ParameterSet.Config as cms\n")
        outFile.write("import pickle\n")
        outFile.write("process = pickle.load(open('PSet.pkl', 'rb'))\n")# % os.path.split(pklFileName)[1])
        outFile.close()

        return


    def outputFiles(self):
        """
        Returns a tuple of lists of output files. First element is PoolOutput files,
        second is TFileService files.
        """
        edmfiles = []
        process = self.fullConfig.process

        #determine all paths/endpaths which will run
        pathsToRun = set()
        if process.schedule is not None:
            for p in process.schedule:
                pathsToRun.add(p.label())

        #determine all modules on EndPaths
        modulesOnEndPaths = set()
        for m in process.endpaths_().itervalues():
             if len(pathsToRun)==0 or m.label() in pathsToRun:
                 for n in m.moduleNames():
                     modulesOnEndPaths.add(n)

        outputModules = set()
        for n,o in process.outputModules_().iteritems():
            if n in modulesOnEndPaths and hasattr(o, 'fileName'):
                edmfiles.append(o.fileName.value().lstrip('file:'))
                outputModules.add(o)

        ## If there are multiple output modules, make sure they have dataset.filterName set.
        if len(outputModules) > 1:
            for outputModule in outputModules:
                try:
                    dataset = getattr(outputModule, 'dataset')
                    filterName = getattr(dataset, 'filterName')
                except AttributeError:
                    raise RuntimeError('Your output module %s does not have a "dataset" PSet ' % outputModule +
                                       'or the PSet does not have a "filterName" member.')

        ## Find files written by TFileService.
        tfiles = []
        if process.services.has_key('TFileService'):
            tFileService = process.services['TFileService']
            if "fileName" in tFileService.parameterNames_():
                tfiles.append(getattr(tFileService, 'fileName').value().lstrip('file:'))

        return edmfiles, tfiles
