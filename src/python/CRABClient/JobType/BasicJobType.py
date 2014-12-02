"""
Abstract class that should be inherited by each job type plug-in
Conventions:
 1) the plug-in file name has to be equal to the plug-in class
 2) a plug-in needs to implement mainly the run method
"""
from ast import literal_eval
from WMCore.Configuration import Configuration
from WMCore.DataStructs.LumiList import LumiList

class BasicJobType(object):
    """
    BasicJobType

    TODO: thinking on having a job type help here...
    """

    def __init__(self, config, logger, workingdir):
        self.logger = logger
        ## Before everything checking if the config is ok
        if config:
            result, msg = self.validateConfig( config )
            if result:
                self.config  = config
                self.workdir = workingdir
            else:
                ## the config was not ok, returning a proper message
                raise Exception( msg )


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
        return (True, '')


    @staticmethod
    def mergeLumis(inputdata, lumimask):
        """
        Computes the processed lumis, merges if needed and returns the compacted list (called when usedbs=no).
        """
        doubleLumis = set()
        mergedLumis = set()

        #merge the lumis from single files
        for reports in inputdata.values():
            for report in reports:
                for run, lumis in literal_eval(report['runlumi']).iteritems():
                    for lumi in lumis:
                        if (run,lumi) in mergedLumis:
                            doubleLumis.add((run,lumi))
                        mergedLumis.add((run,lumi))

        #convert the runlumis from list of pairs to dict: [(123,3), (123,4), (123,5), (123,7), (234,6)] => {123 : [3,4,5,7], 234 : [6]}
        dLumisDict = {}
        mLumisDict = {}
        for k, v in doubleLumis:
            dLumisDict.setdefault(k, []).append(int(v))
        for k, v in mergedLumis:
            mLumisDict.setdefault(k, []).append(int(v))

        doubleLumis = LumiList(runsAndLumis=dLumisDict)
        mergedLumis = LumiList(runsAndLumis=mLumisDict)

        #get the compact list using CMSSW framework
        return mergedLumis.getCompactList(), (LumiList(compactList=lumimask) - mergedLumis).getCompactList(), doubleLumis.getCompactList()

    @staticmethod
    def subtractLumis(input, output):
        """
        Computes the processed lumis, merges from the DBS reuslts (called when usedbs=yes).
        """
        out = LumiList(runsAndLumis=output)
        in_ = LumiList(runsAndLumis=input)
        diff = in_ - out

        #calculate lumis counted twice
        doubleLumis = set()
        for run,lumis in output.iteritems():
            for lumi in lumis:
                if output[run].count(lumi) > 1:
                    doubleLumis.add((run,lumi))
        dLumisDict = {}
        for k, v in doubleLumis:
            dLumisDict.setdefault(k, []).append(v)
        double = LumiList(runsAndLumis=dLumisDict)

        return out.getCompactList(), diff.getCompactList(), double.getCompactList()
