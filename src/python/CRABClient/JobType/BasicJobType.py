"""
Abstract class that should be inherited by each job type plug-in
Conventions:
 1) the plug-in file name has to be equal to the plug-in class
 2) a plug-in needs to implement mainly the run method
"""
from ast import literal_eval

from WMCore.DataStructs.LumiList import LumiList

from CRABClient.ClientExceptions import ConfigurationException


class BasicJobType(object):
    """
    BasicJobType

    TODO: thinking on having a job type help here...
    """

    def __init__(self, config, proxyfilename, logger, workingdir, crabserver, s3tester):
        self.logger = logger
        self.proxyfilename = proxyfilename
        self.automaticAvail = False
        self.crabserver = crabserver
        self.s3tester = s3tester
        ## Before everything, check if the config is ok.
        if config:
            valid, msg = self.validateConfig(config)
            if valid:
                self.config = config
                self.workdir = workingdir
            else:
                msg += "\nThe documentation about the CRAB configuration file can be found in"
                msg += " https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3ConfigurationFile"
                raise ConfigurationException(msg)


    def run(self):
        """
        _run_

        Here goes the job type algorithm
        """
        raise NotImplementedError()


    def validateConfig(self, config):
        """
        _validateConfig_

        Allows to have a basic validation of the needed parameters
        """
        ## (boolean with the result of the validation, eventual error message)
        return True, "Valid configuration"


    @staticmethod
    def mergeLumis(inputdata):
        """
        Computes the processed lumis, merges if needed and returns the compacted list.
        """
        mergedLumis = set()
        #merge the lumis from single files
        for reports in inputdata.values():
            for report in reports:
                for run, lumis in literal_eval(report['runlumi']).items():
                    if isinstance(run, bytes):
                        run = run.decode(encoding='UTF-8')
                    for lumi in lumis:
                        mergedLumis.add((run, int(lumi))) #lumi is str, but need int
        mergedLumis = LumiList(lumis=mergedLumis)
        return mergedLumis.getCompactList()


    @staticmethod
    def intersectLumis(lumisA, lumisB):
        result = LumiList(compactList=lumisA) & LumiList(compactList=lumisB)
        return result.getCompactList()


    @staticmethod
    def subtractLumis(lumisA, lumisB):
        result = LumiList(compactList=lumisA) - LumiList(compactList=lumisB)
        return result.getCompactList()


    @staticmethod
    def getDuplicateLumis(lumisDict):
        """
        Get the run-lumis appearing more than once in the input
        dictionary of runs and lumis, which is assumed to have
        the following format:
            {
            '1': [1,2,3,4,6,7,8,9,10],
            '2': [1,4,5,20]
            }
        """
        doubleLumis = set()
        for run, lumis in lumisDict.items():
            seen = set()
            doubleLumis.update(set((run, lumi) for lumi in lumis if (run, lumi) in seen or seen.add((run, lumi))))
        doubleLumis = LumiList(lumis=doubleLumis)
        return doubleLumis.getCompactList()
