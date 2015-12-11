"""
Abstract class that should be inherited by each job type plug-in
Conventions:
 1) the plug-in file name has to be equal to the plug-in class
 2) a plug-in needs to implement mainly the run method
"""
from ast import literal_eval
from WMCore.Configuration import Configuration
from WMCore.DataStructs.LumiList import LumiList
from CRABClient.ClientExceptions import ConfigurationException


class BasicJobType(object):
    """
    BasicJobType

    TODO: thinking on having a job type help here...
    """

    def __init__(self, config, logger, workingdir):
        self.logger = logger
        ## Before everything, check if the config is ok.
        if config:
            valid, msg = self.validateConfig(config)
            if valid:
                self.config  = config
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


    def validateConfig(self):
        """
        _validateConfig_

        Allows to have a basic validation of the needed parameters
        """
        ## (boolean with the result of the validation, eventual error message)
        return True, "Valid configuration"


    @staticmethod
    def mergeLumis(inputdata, lumimask):
        """
        Computes the processed lumis, merges if needed and returns the compacted list (called when usedbs=no).
        """
        mergedLumis = set()
        #merge the lumis from single files
        for reports in inputdata.values():
            for report in reports:
                for run, lumis in literal_eval(report['runlumi']).iteritems():
                    for lumi in lumis:
                        mergedLumis.add((run,int(lumi))) #lumi is str, but need int
        mergedLumis = LumiList(lumis=mergedLumis)
        diff = LumiList(compactList=lumimask) - mergedLumis
        return mergedLumis.getCompactList(), diff.getCompactList()


    @staticmethod
    def subtractLumis(input, output):
        """
        Computes the processed lumis, merges from the DBS reuslts (called when usedbs=yes).
        """
        out = LumiList(runsAndLumis=output)
        in_ = LumiList(runsAndLumis=input)
        diff = in_ - out
        return out.getCompactList(), diff.getCompactList()


    @staticmethod
    def getDoubleLumis(lumisDict):
        #calculate lumis counted twice
        doubleLumis = set()
        for run, lumis in lumisDict.iteritems():
            seen = set()
            doubleLumis.update(set((run, lumi) for lumi in lumis if (run, lumi) in seen or seen.add((run, lumi))))
        doubleLumis = LumiList(lumis=doubleLumis)
        return doubleLumis.getCompactList()
