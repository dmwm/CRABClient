"""
Module to handle CMSSW _cfg.py file
"""

import re
import os
import sys
import imp
import json
import pickle
import logging

from FWCore.ParameterSet.Types import uint32

from ServerUtilities import BOOTSTRAP_CFGFILE_DUMP

from CRABClient.ClientExceptions import ConfigurationException, EnvironmentException
from CRABClient.ClientUtilities import bootstrapDone, colors, BOOTSTRAP_CFGFILE_PKL,\
    BOOTSTRAP_INFOFILE, LOGGERS, PKL_R_MODE, PKL_W_MODE

# cache user configuration to speed up things in case this is called multiple times in same process
# via CRAB command API. Note that CMSSW configuration can only be loaded once in memory !
configurationCache = {}


class CMSSWConfig(object):
    """
    Class to handle CMSSW _cfg.py file
    """
    def __init__(self, config, userConfig=None, logger=None):
        global configurationCache  # pylint: disable=global-statement
        self.config = config
        self.logger = logger if logger else logging

        self.fullConfig = None
        self.outputFile = None

        if userConfig:
            cfgBaseName = os.path.basename(userConfig).replace(".py", "")
            cfgDirName = os.path.dirname(os.path.abspath(userConfig))

            if not os.path.isfile(userConfig):
                msg = "\nCannot find CMSSW configuration file %s in %s" % (userConfig, os.getcwd())
                raise ConfigurationException(msg)

            self.logger.info("Importing CMSSW configuration %s" % (userConfig))
            pyCfgParams = getattr(self.config.JobType, 'pyCfgParams', [])
            originalArgv = sys.argv
            sys.argv = [userConfig]
            if pyCfgParams:
                sys.argv.extend(pyCfgParams)
                msg = "Additional parameters for the CMSSW configuration are: %s" % (pyCfgParams)
                self.logger.debug(msg)

            #Add cmssw python paths
            if 'CRAB_CMSSW_SITE_PACKAGES' in os.environ:
                import site
                for p in os.environ['CRAB_CMSSW_SITE_PACKAGES'].split(os.pathsep):
                    site.addsitedir(p)

            # details of user configuration file:
            configFile, pathname, description = imp.find_module(cfgBaseName, [cfgDirName])
            cacheLine = (pathname, tuple(sys.argv))
            cwd=os.getcwd()
            if cwd not in sys.path:   # cwd must be part of $PYTHONPATH for CMSSW config to work
                sys.path.append(cwd)

            if configurationCache:
                # check that nothing has changed, and use it
                if not cacheLine in configurationCache:
                    msg = "\nFATAL ERROR: A different CMSSW configuration was already cached."
                    msg += "\n Either configuration file name or configuration parameters have been changed."
                    msg += "\n But CMSSW configuration files can't be loaded more than once in memory."
                    raise ConfigurationException(msg)
                if tuple(sys.path) != configurationCache[cacheLine]['path']:
                    msg = '\nFATAL ERROR: sys.path ($PYTHONPATH) has changed since CMSSW configuration file was loaded.'
                    raise ConfigurationException(msg)

                self.fullConfig = configurationCache[cacheLine]['config']
            else:
                # cache this configuration
                if cwd not in sys.path:
                    sys.path.append(cwd)
                try:
                    oldstdout = sys.stdout
                    sys.stdout = open(logger.logfile, 'a')
                    self.fullConfig = imp.load_module(cfgBaseName, configFile, pathname, description)
                finally:
                    sys.stdout.close()
                    sys.stdout = oldstdout
                    configFile.close()
                # need to turn sys.path into a static set of strings for using it as a cache key
                # otherwise is a pointer to a function and we can't use it to check for stability
                configurationCache[cacheLine] = { 'config' : self.fullConfig , 'path' : tuple(sys.path) }
            self.logger.info("Finished importing CMSSW configuration %s" % (userConfig))
            sys.argv = originalArgv


    def writeFile(self, filename = 'CMSSW.py'):
        """
        Persist fully expanded _cfg.py file
        """

        self.outputFile = filename
        self.logger.debug("Writing CMSSW configuration to %s" % self.outputFile)

        basedir = os.path.dirname(filename)

        #saving the process object as a pickle
        pklFileName = os.path.join(basedir, BOOTSTRAP_CFGFILE_PKL)
        pklFile = open(pklFileName, PKL_W_MODE)
        pickle.dump(self.fullConfig.process, pklFile, protocol=0)
        pklFile.close()

        #create the auxiliary file
        outFile = open(filename, "w")
        outFile.write("import FWCore.ParameterSet.Config as cms\n")
        outFile.write("import pickle\n")
        outFile.write("process = pickle.load(open('PSet.pkl', \'%s\'))\n" % PKL_R_MODE)
        outFile.close()

        try:
            dumpedStr = self.fullConfig.process.dumpPython()
            dumpFileName = os.path.join(basedir, BOOTSTRAP_CFGFILE_DUMP)
            with open(dumpFileName, 'w') as fd:
                fd.write(dumpedStr)
        except Exception as e:
            LOGGERS['CRAB3'].error(str(e))
            raise ConfigurationException('\nFATAL ERROR, cannot dump CMSSW configuration file. This prevents sandbox recycling.')

        return


    def hasPoolSource(self):
        """
        Returns if an PoolSource is present in the parameter set.
        """
        if bootstrapDone():
            self.logger.debug("Getting source info from bootstrap cachefile.")
            info = self.getCfgInfo()
            return info['poolinfo']

        isPool = False

        if getattr(self.fullConfig.process, 'source'):
            source = self.fullConfig.process.source
            try:
                isPool = str(source.type_()) == 'PoolSource'
            except AttributeError as ex:
                msg = "\nFATAL ERROR: Invalid CMSSW configuration: Failed to check if 'process.source' is of type 'PoolSource': %s" % (ex)
                raise ConfigurationException(msg)

        return isPool


    def hasLHESource(self):
        """
        Returns a tuple containing a bool to indicate usage of an
        LHESource and an integer for the number of input files.
        """
        if bootstrapDone():
            self.logger.debug("Getting lhe info from bootstrap cachefile.")
            info = self.getCfgInfo()
            return info['lheinfo']

        isLHE, numFiles = False, 0

        if getattr(self.fullConfig.process, 'source'):
            source = self.fullConfig.process.source
            try:
                isLHE = str(source.type_()) == 'LHESource'
            except AttributeError as ex:
                msg = "\nInvalid CMSSW configuration: Failed to check if 'process.source' is of type 'LHESource': %s" % (ex)
                raise ConfigurationException(msg)
            if isLHE:
                if hasattr(source, 'fileNames'):
                    numFiles = len(source.fileNames)
                else:
                    msg = "\nInvalid CMSSW configuration: Object 'process.source', of type 'LHESource', is missing attribute 'fileNames'."
                    raise ConfigurationException(msg)

        return isLHE, numFiles


    def outputFiles(self):
        """
        Returns a tuple of lists of output files. First element is PoolOutput files,
        second is TFileService files.
        """
        if bootstrapDone():
            self.logger.debug("Getting output files from bootstrap cachefile.")
            info = self.getCfgInfo()
            return info['outfiles']

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

        # add modules in FinalPaths if available
        if hasattr(process, "finalpaths_"):
            for m in process.finalpaths_().itervalues():
                if len(pathsToRun)==0 or m.label() in pathsToRun:
                    for n in m.moduleNames():
                        modulesOnEndPaths.add(n)

        outputModules = set()
        for n,o in process.outputModules_().items():
            if n in modulesOnEndPaths and hasattr(o, 'fileName'):
                edmfile = re.sub(r'^file:', '', o.fileName.value())
                edmfile = os.path.basename(edmfile)
                edmfiles.append(edmfile)
                outputModules.add(o)

        ## If there are multiple output modules, make sure they have dataset.filterName set.
        if len(outputModules) > 1:
            for outputModule in outputModules:
                try:
                    dataset = getattr(outputModule, 'dataset')
                    getattr(dataset, 'filterName')
                except AttributeError:
                    raise RuntimeError('\nYour output module %s does not have a "dataset" PSet ' % outputModule.label() +
                                       'or the PSet does not have a "filterName" member.')

        ## Find files written by TFileService.
        tfiles = []
        if 'TFileService' in process.services:
            tFileService = process.services['TFileService']
            if "fileName" in tFileService.parameterNames_():
                tfile = re.sub(r'^file:', '', getattr(tFileService, 'fileName').value())
                tfile = os.path.basename(tfile)
                tfiles.append(tfile)

        return edmfiles, tfiles


    def getCfgInfo(self):
        bootFilename = os.path.join(os.environ['CRAB3_BOOTSTRAP_DIR'], BOOTSTRAP_INFOFILE)
        if not os.path.isfile(bootFilename):
            msg = "\nThe CRAB3_BOOTSTRAP_DIR environment variable is set, but I could not find %s" % bootFilename
            raise EnvironmentException(msg)
        else:
            with open(bootFilename) as fd:
                return json.load(fd)


    def validateConfig(self):
        """ Do a basic validation of the CMSSW parameter-set configuration.
        """
        if not self.fullConfig:
            msg = "Validation of CMSSW configuration was requested, but there is no configuration to validate."
            return False, msg

        if not getattr(self.fullConfig, 'process', None):
            msg = "Invalid CMSSW configuration: 'process' object is missing or is wrongly defined."
            return False, msg

        if not getattr(self.fullConfig.process, 'source', None):
            msg = "Invalid CMSSW configuration: 'process' object is missing attribute 'source' or the attribute is wrongly defined."
            return False, msg

        #Assumes a default of 1 if the parameter is not specified
        cfgNumCores = getattr(self.config.JobType, 'numCores', 1)
        numPSetCores = getattr(getattr(self.fullConfig.process, 'options', object), 'numberOfThreads', uint32(1))
        if numPSetCores and not isinstance(numPSetCores, uint32):
            msg = "The only accepted type for process.options.numberOfThreads is uint32. Please, change its type in the PSet."
            msg += "See https://github.com/cms-sw/cmssw/issues/32070 for details"
            return False, msg
        if (cfgNumCores or 1) != (numPSetCores or 1):
            if cfgNumCores == None:
                msg = "You did not set config.JobType.numCores in the crab configuration file "
            else:
                msg = "You specified config.JobType.numCores=%s in the crab configuration file " % cfgNumCores
            if numPSetCores == None:
                msg += "but process.options.numberOfThreads is not specified. "
            else:
                msg += "but process.options.numberOfThreads=%s. " % numPSetCores
            msg += "Please make sure the two parameters are consistent and have the same value (or they are both missing) "
            ##MM In the following message it is not simple to get the CRAB cfg filename: you have to take care
            ##of the case of the CRAB library where the cfg is an object. I think it is good as it is now
            msg += "in the crab configuration file and in the CMSSW PSet (%s)" % self.config.JobType.psetName
            return False, msg
        # At this point cfgNumCores and numPSetCores are the same
        if numPSetCores not in [None, 1, 2, 4, 8]:
            msg = "The only values allowed for config.JobType.numCores are 1, 2, 4, 8"
            return False, msg
        elif numPSetCores > 1:
            self.logger.info("%sYou are requesting more than 1 core per job. Please make sure that your multi-threaded code is thread-safe and CPU-efficient, and request the correct amount of memory since the default 'maxMemoryMB' value is meant for single core jobs.%s" % (colors.RED, colors.NORMAL))

        return True, "Valid configuration"

